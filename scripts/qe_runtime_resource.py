"""Runner-side QE phase resource sampler and authenticated event publisher."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

try:
    import psutil
except ModuleNotFoundError:  # pragma: no cover - exercised on minimal compute images
    psutil = None


RESOURCE_FILE = "qe_runtime_resource.json"
UPLOAD_FAILURE_FILE = "qe_runtime_resource_upload_failure.json"
RESOURCE_SECRET_FILE = "qe_resource_session_secret.json"
BASE_URL_ENVS = (
    "AISTOCK_PREDICTION_STORE_BASE_URL",
    "AISTOCK_QE_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_BASE_URL",
)
_MONITOR: "QERuntimeResourceMonitor | None" = None
_RESOURCE_SECRET_CACHE: dict[str, Any] | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _resource_secret() -> dict[str, Any]:
    global _RESOURCE_SECRET_CACHE
    if _RESOURCE_SECRET_CACHE is not None:
        return _RESOURCE_SECRET_CACHE
    path = Path.cwd() / RESOURCE_SECRET_FILE
    if not path.exists():
        _RESOURCE_SECRET_CACHE = {}
        return _RESOURCE_SECRET_CACHE
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{RESOURCE_SECRET_FILE} must contain a JSON object")
    _RESOURCE_SECRET_CACHE = payload
    return _RESOURCE_SECRET_CACHE


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if not raw:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean, got {raw!r}")


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    value = float(raw)
    if value <= 0:
        raise RuntimeError(f"{name} must be positive, got {value}")
    return value


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


@dataclass
class _PhaseAggregate:
    phase: str
    started_at: str = field(default_factory=_utc_now)
    started_monotonic: float = field(default_factory=time.monotonic)
    sample_count: int = 0
    process_rss_peak_bytes: int = 0
    process_vm_hwm_peak_bytes: int = 0
    gpu_memory_used_peak_bytes: int = 0
    gpu_process_memory_peak_bytes: int = 0
    gpu_utilization_sum_pct: float = 0.0
    gpu_utilization_peak_pct: float = 0.0
    cuda_allocated_peak_bytes: int = 0
    cuda_reserved_peak_bytes: int = 0
    gpu_device_index: int | None = None
    gpu_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def observe(self, sample: dict[str, Any]) -> None:
        self.sample_count += 1
        self.process_rss_peak_bytes = max(self.process_rss_peak_bytes, int(sample.get("process_rss_bytes") or 0))
        self.process_vm_hwm_peak_bytes = max(
            self.process_vm_hwm_peak_bytes,
            int(sample.get("process_vm_hwm_bytes") or 0),
        )
        self.gpu_memory_used_peak_bytes = max(
            self.gpu_memory_used_peak_bytes,
            int(sample.get("gpu_memory_used_bytes") or 0),
        )
        self.gpu_process_memory_peak_bytes = max(
            self.gpu_process_memory_peak_bytes,
            int(sample.get("gpu_process_memory_bytes") or 0),
        )
        utilization = float(sample.get("gpu_utilization_pct") or 0.0)
        self.gpu_utilization_sum_pct += utilization
        self.gpu_utilization_peak_pct = max(self.gpu_utilization_peak_pct, utilization)
        self.cuda_allocated_peak_bytes = max(
            self.cuda_allocated_peak_bytes,
            int(sample.get("cuda_allocated_bytes") or 0),
        )
        self.cuda_reserved_peak_bytes = max(
            self.cuda_reserved_peak_bytes,
            int(sample.get("cuda_reserved_bytes") or 0),
        )
        if sample.get("gpu_device_index") is not None:
            self.gpu_device_index = int(sample["gpu_device_index"])
        if sample.get("gpu_name"):
            self.gpu_name = str(sample["gpu_name"])

    def event_fields(self) -> dict[str, Any]:
        ended_at = _utc_now()
        return {
            "started_at": self.started_at,
            "ended_at": ended_at,
            "duration_seconds": max(0.0, time.monotonic() - self.started_monotonic),
            "sample_count": self.sample_count,
            "process_rss_peak_bytes": self.process_rss_peak_bytes or None,
            "process_vm_hwm_peak_bytes": self.process_vm_hwm_peak_bytes or None,
            "gpu_device_index": self.gpu_device_index,
            "gpu_name": self.gpu_name,
            "gpu_memory_used_peak_bytes": self.gpu_memory_used_peak_bytes or None,
            "gpu_process_memory_peak_bytes": self.gpu_process_memory_peak_bytes or None,
            "gpu_utilization_avg_pct": (
                self.gpu_utilization_sum_pct / self.sample_count if self.sample_count else None
            ),
            "gpu_utilization_peak_pct": self.gpu_utilization_peak_pct if self.sample_count else None,
            "cuda_allocated_peak_bytes": self.cuda_allocated_peak_bytes or None,
            "cuda_reserved_peak_bytes": self.cuda_reserved_peak_bytes or None,
            "metadata": dict(self.metadata),
        }


class QERuntimeResourceMonitor:
    def __init__(self) -> None:
        secret = _resource_secret()
        self.session_id = _env("QE_RESOURCE_SESSION_ID") or str(secret.get("session_id") or "")
        self.source_run_key = _env("QE_RESOURCE_SOURCE_RUN_KEY") or str(secret.get("source_run_key") or "")
        self.token = _env("QE_RESOURCE_SESSION_TOKEN") or str(secret.get("token") or "")
        self.task_id = _env("QE_TASK_ID")
        self.loop_id = _env("QE_LOOP_ID")
        self.loop_index = int(_env("QE_LOOP_INDEX"))
        self.node_id = _env("QE_NODE_ID") or _env("AISTOCK_NODE_ID")
        self.phase_pipeline_enabled = _env_bool("QE_PHASE_PIPELINE_ENABLED", False)
        self.sample_interval = _env_float("QE_RESOURCE_SAMPLE_INTERVAL_SEC", 1.0)
        self.upload_timeout = _env_float("QE_RESOURCE_UPLOAD_TIMEOUT_SEC", 10.0)
        self.url = self._resolve_url()
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._phase = _PhaseAggregate("bootstrap")
        self._sequence_no = 0
        self._events: list[dict[str, Any]] = []
        self._upload_broken = False
        self._finished = False

    @classmethod
    def from_env(cls) -> "QERuntimeResourceMonitor | None":
        secret = _resource_secret()
        required = {
            "QE_RESOURCE_SESSION_ID": _env("QE_RESOURCE_SESSION_ID") or secret.get("session_id"),
            "QE_RESOURCE_SOURCE_RUN_KEY": _env("QE_RESOURCE_SOURCE_RUN_KEY") or secret.get("source_run_key"),
            "QE_RESOURCE_SESSION_TOKEN": _env("QE_RESOURCE_SESSION_TOKEN") or secret.get("token"),
            "QE_TASK_ID": _env("QE_TASK_ID"),
            "QE_LOOP_ID": _env("QE_LOOP_ID"),
            "QE_LOOP_INDEX": _env("QE_LOOP_INDEX"),
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            if _env_bool("QE_PHASE_PIPELINE_ENABLED", False):
                print(
                    "[ERROR] reason_code=QE_GPU_PHASE_HELPER_MISSING "
                    f"missing_env={missing}; GPU phase release will remain fail-closed"
                )
            return None
        return cls()

    def _resolve_url(self) -> str:
        base = next((_env(name) for name in BASE_URL_ENVS if _env(name)), "")
        if not base:
            raise RuntimeError(
                "QE_RESOURCE_EVENT_UPLOAD_FAILED: no backend base URL configured for runtime resource events"
            )
        parsed = urlparse(base)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise RuntimeError(f"QE resource base must be absolute http(s), got {base!r}")
        base = base.rstrip("/")
        path = "/api/v1/quantevolver/evolution/webhook/loop-resource-phase"
        if base.endswith("/api/v1"):
            return f"{base}{path.removeprefix('/api/v1')}"
        return f"{base}{path}"

    def start(self) -> None:
        self._sample_once()
        self._thread = threading.Thread(target=self._sample_loop, name="qe-resource-sampler", daemon=True)
        self._thread.start()
        self._write_local()

    def _sample_loop(self) -> None:
        while not self._stop.wait(self.sample_interval):
            self._sample_once()

    def _sample_once(self) -> None:
        try:
            sample = self._collect_sample()
        except Exception as exc:  # telemetry failure is loud but must not change the experiment result
            with self._lock:
                self._phase.metadata.setdefault("sample_errors", []).append(f"{type(exc).__name__}: {exc}")
            print(f"[ERROR] reason_code=QE_RESOURCE_SAMPLE_FAILED error={type(exc).__name__}: {exc}")
            return
        with self._lock:
            self._phase.observe(sample)

    def _collect_sample(self) -> dict[str, Any]:
        sample = self._process_sample()
        sample.update(self._gpu_sample())
        sample.update(self._torch_sample())
        return sample

    @staticmethod
    def _read_vm_hwm(pid: int) -> int:
        try:
            for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            return 0
        return 0

    def _process_sample(self) -> dict[str, Any]:
        pid = os.getpid()
        if psutil is None:
            return {
                "process_rss_bytes": 0,
                "process_vm_hwm_bytes": self._read_vm_hwm(pid),
                "process_pids": [pid],
                "process_sampler": "proc_current_only",
            }
        root = psutil.Process(pid)
        processes = [root, *root.children(recursive=True)]
        rss = 0
        hwm = 0
        pids: list[int] = []
        for process in processes:
            try:
                pids.append(process.pid)
                rss += int(process.memory_info().rss)
                hwm = max(hwm, self._read_vm_hwm(process.pid))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return {
            "process_rss_bytes": rss,
            "process_vm_hwm_bytes": hwm,
            "process_pids": pids,
            "process_sampler": "psutil_process_tree",
        }

    def _gpu_sample(self) -> dict[str, Any]:
        gpu_index = int(_env("QE_GPU_DEVICE_INDEX") or 0)
        result: dict[str, Any] = {"gpu_device_index": gpu_index}
        query = subprocess.run(
            [
                "nvidia-smi",
                f"--id={gpu_index}",
                "--query-gpu=name,memory.used,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if query.returncode == 0 and query.stdout.strip():
            name, memory_mib, utilization = [part.strip() for part in query.stdout.splitlines()[0].split(",", 2)]
            result.update(
                {
                    "gpu_name": name,
                    "gpu_memory_used_bytes": int(float(memory_mib)) * 1024 * 1024,
                    "gpu_utilization_pct": float(utilization),
                }
            )

        process_query = subprocess.run(
            [
                "nvidia-smi",
                "--query-compute-apps=pid,used_memory",
                "--format=csv,noheader,nounits",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if process_query.returncode == 0:
            process_pids = set(self._process_sample().get("process_pids") or [])
            process_memory_mib = 0
            for line in process_query.stdout.splitlines():
                parts = [part.strip() for part in line.split(",", 1)]
                if len(parts) != 2:
                    continue
                try:
                    if int(parts[0]) in process_pids:
                        process_memory_mib += int(float(parts[1]))
                except ValueError:
                    continue
            result["gpu_process_memory_bytes"] = process_memory_mib * 1024 * 1024
        return result

    @staticmethod
    def _torch_sample() -> dict[str, Any]:
        torch = sys.modules.get("torch")
        if torch is None or not getattr(torch, "cuda", None) or not torch.cuda.is_available():
            return {}
        return {
            "cuda_allocated_bytes": int(torch.cuda.memory_allocated()),
            "cuda_reserved_bytes": int(torch.cuda.memory_reserved()),
        }

    def transition(self, next_phase: str, *, metadata: dict[str, Any] | None = None) -> None:
        with self._lock:
            if self._finished:
                return
            self._publish_current("completed")
            self._phase = _PhaseAggregate(next_phase, metadata=dict(metadata or {}))
            self._sample_once()

    def record_resident_state(
        self,
        *,
        requested: bool,
        active: bool,
        fallback_reason_code: str | None = None,
    ) -> None:
        with self._lock:
            self._phase.metadata.update(
                {
                    "resident_requested": bool(requested),
                    "resident_active": bool(active),
                    "resident_fallback": bool(fallback_reason_code),
                    "fallback_reason_code": fallback_reason_code,
                }
            )

    def release_gpu_phase(self, *, proof: dict[str, Any]) -> bool:
        with self._lock:
            self._publish_current("completed")
            passed = bool(proof.get("release_check_passed"))
            phase = "gpu_phase_released" if passed else "release_rejected"
            reason_code = (
                "QE_GPU_PHASE_RELEASE_CONFIRMED"
                if passed
                else "QE_GPU_PHASE_RELEASE_THRESHOLD_EXCEEDED"
            )
            release_event = self._base_event(phase, "released" if passed else "rejected")
            release_event.update(proof)
            release_event["release_check_passed"] = passed
            release_event["reason_code"] = reason_code
            release_event["cuda_allocated_end_bytes"] = proof.get("cuda_allocated_bytes_after")
            release_event["cuda_reserved_end_bytes"] = proof.get("cuda_reserved_bytes_after")
            release_event["metadata"] = {
                key: value
                for key, value in proof.items()
                if key
                not in {
                    "release_check_passed",
                    "cuda_allocated_bytes_after",
                    "cuda_reserved_bytes_after",
                }
            }
            self._publish(release_event)
            self._phase = _PhaseAggregate("backtest")
            self._sample_once()
            return passed and not self._upload_broken

    def finish(self, *, status: str, error: str | None = None) -> None:
        with self._lock:
            if self._finished:
                return
            terminal = "completed" if status == "completed" else "failed"
            if error:
                self._phase.metadata["terminal_error"] = error
            self._publish_current(terminal)
            terminal_event = self._base_event(terminal, terminal)
            terminal_event["reason_code"] = (
                "QE_RESOURCE_RUN_COMPLETED" if terminal == "completed" else "QE_RESOURCE_RUN_FAILED"
            )
            if error:
                terminal_event["metadata"] = {"error": error}
            self._publish(terminal_event)
            self._finished = True
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.sample_interval * 2))
        self._write_local()

    def _publish_current(self, phase_status: str) -> None:
        sample = self._torch_sample()
        fields = self._phase.event_fields()
        event = self._base_event(self._phase.phase, phase_status)
        event.update(fields)
        event["cuda_allocated_end_bytes"] = sample.get("cuda_allocated_bytes")
        event["cuda_reserved_end_bytes"] = sample.get("cuda_reserved_bytes")
        resident = fields.get("metadata") or {}
        for key in (
            "resident_requested",
            "resident_active",
            "resident_fallback",
            "fallback_reason_code",
        ):
            if key in resident:
                event[key] = resident[key]
        self._publish(event)

    def _base_event(self, phase: str, phase_status: str) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "source_run_key": self.source_run_key,
            "task_id": self.task_id,
            "loop_id": self.loop_id,
            "loop_index": self.loop_index,
            "node_id": self.node_id,
            "sequence_no": self._sequence_no + 1,
            "phase": phase,
            "phase_status": phase_status,
        }

    def _publish(self, event: dict[str, Any]) -> None:
        if self._upload_broken:
            event = dict(event)
            event["upload_skipped_reason_code"] = "QE_RESOURCE_EVENT_UPLOAD_FAILED"
            self._events.append(event)
            self._write_local()
            return
        error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = requests.post(
                    self.url,
                    json=event,
                    headers={"X-QE-Resource-Token": self.token},
                    timeout=self.upload_timeout,
                )
                if response.status_code >= 400:
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
                self._sequence_no = int(event["sequence_no"])
                self._events.append(dict(event))
                self._write_local()
                return
            except Exception as exc:  # retry exact same payload for idempotency
                error = exc
                if attempt < 3:
                    time.sleep(min(2.0, 0.25 * (2 ** (attempt - 1))))
        self._upload_broken = True
        marker = {
            "schema_version": "qe_runtime_resource_upload_failure_v1",
            "reason_code": "QE_RESOURCE_EVENT_UPLOAD_FAILED",
            "session_id": self.session_id,
            "sequence_no": event.get("sequence_no"),
            "phase": event.get("phase"),
            "error": f"{type(error).__name__}: {error}",
            "written_at": _utc_now(),
        }
        _atomic_json(Path.cwd() / UPLOAD_FAILURE_FILE, marker)
        failed_event = dict(event)
        failed_event["upload_error"] = marker["error"]
        self._events.append(failed_event)
        self._write_local()
        print(
            "[ERROR] reason_code=QE_RESOURCE_EVENT_UPLOAD_FAILED "
            f"session_id={self.session_id} phase={event.get('phase')} error={marker['error']}"
        )

    def _write_local(self) -> None:
        payload = {
            "schema_version": "qe_runtime_resource_v1",
            "session_id": self.session_id,
            "source_run_key": self.source_run_key,
            "task_id": self.task_id,
            "loop_id": self.loop_id,
            "loop_index": self.loop_index,
            "node_id": self.node_id,
            "phase_pipeline_enabled": self.phase_pipeline_enabled,
            "current_phase": self._phase.phase,
            "last_sequence_no": self._sequence_no,
            "upload_broken": self._upload_broken,
            "events": list(self._events),
            "updated_at": _utc_now(),
        }
        _atomic_json(Path.cwd() / RESOURCE_FILE, payload)


def start_resource_monitor() -> QERuntimeResourceMonitor | None:
    global _MONITOR
    if _MONITOR is not None:
        return _MONITOR
    try:
        monitor = QERuntimeResourceMonitor.from_env()
        if monitor is not None:
            monitor.start()
        _MONITOR = monitor
        return monitor
    except Exception as exc:
        print(f"[ERROR] reason_code=QE_RESOURCE_MONITOR_START_FAILED error={type(exc).__name__}: {exc}")
        if _env_bool("QE_PHASE_PIPELINE_ENABLED", False):
            print("[ERROR] GPU phase release will remain fail-closed")
        return None


def transition_resource_phase(phase: str, *, metadata: dict[str, Any] | None = None) -> None:
    if _MONITOR is not None:
        _MONITOR.transition(phase, metadata=metadata)


def record_gpu_resident_state(
    *,
    requested: bool,
    active: bool,
    fallback_reason_code: str | None = None,
) -> None:
    if _MONITOR is not None:
        _MONITOR.record_resident_state(
            requested=requested,
            active=active,
            fallback_reason_code=fallback_reason_code,
        )


def publish_gpu_phase_release(proof: dict[str, Any]) -> bool:
    if _MONITOR is None:
        if _env_bool("QE_PHASE_PIPELINE_ENABLED", False):
            print("[ERROR] reason_code=QE_GPU_PHASE_HELPER_MISSING release_event_not_published=true")
        return False
    return _MONITOR.release_gpu_phase(proof=proof)


def finish_resource_monitor(*, status: str, error: str | None = None) -> None:
    if _MONITOR is not None:
        _MONITOR.finish(status=status, error=error)
