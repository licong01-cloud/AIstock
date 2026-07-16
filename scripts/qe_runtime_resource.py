"""Runner-side QE phase resource sampler and authenticated event publisher."""

from __future__ import annotations

import gc
import json
import os
import platform
import re
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
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

try:
    import pynvml
except ModuleNotFoundError:  # pragma: no cover - exercised on CPU-only/minimal compute images
    pynvml = None

if os.name == "nt":  # pragma: no cover - production QE runners are Linux/WSL
    import msvcrt
else:  # pragma: no cover - covered by the WSL runtime canary
    import fcntl


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
_NVML_LOCK = threading.Lock()
_NVML_INITIALIZED = False
_NVML_HANDLES: dict[int, Any] = {}
_GPU_CACHE_SCHEMA_VERSION = "qe_node_gpu_snapshot_v1"
_WSL_GPU_QUERY_SUPPRESSED_REASON = "QE_RESOURCE_GPU_DEVICE_QUERY_SUPPRESSED_WSL_DXG"
_DEFAULT_GPU_PHASE_RELEASE_TOLERANCE_BYTES = 256 * 1024**2
_GPU_RELEASE_NEXT_PHASES = {"backtest", "finalize"}
_PHASE_EVENT_STATE = threading.local()


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


@contextmanager
def _exclusive_file_lock(path: Path):
    """Cross-process node lock used only for the shared GPU snapshot."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":  # pragma: no cover - production QE runners are Linux/WSL
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:  # pragma: no cover - covered by the WSL runtime canary
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _safe_cache_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "unknown")).strip("._")
    return normalized or "unknown"


def _is_wsl_runtime() -> bool:
    if _env("WSL_INTEROP") or _env("WSL_DISTRO_NAME"):
        return True
    return "microsoft" in platform.release().lower()


def _nvml_device_snapshot(gpu_index: int) -> dict[str, Any]:
    """Query NVML through one persistent process-local handle, never a subprocess."""

    global _NVML_INITIALIZED
    if _is_wsl_runtime():
        raise RuntimeError(
            f"{_WSL_GPU_QUERY_SUPPRESSED_REASON}: NVML adapter queries are forbidden inside WSL QE runners"
        )
    if pynvml is None:
        raise RuntimeError(
            "QE_RESOURCE_NVML_UNAVAILABLE: pynvml is required for GPU telemetry; "
            "nvidia-smi subprocess fallback is intentionally disabled"
        )
    with _NVML_LOCK:
        if not _NVML_INITIALIZED:
            pynvml.nvmlInit()
            _NVML_INITIALIZED = True
        handle = _NVML_HANDLES.get(gpu_index)
        if handle is None:
            handle = pynvml.nvmlDeviceGetHandleByIndex(gpu_index)
            _NVML_HANDLES[gpu_index] = handle
        raw_name = pynvml.nvmlDeviceGetName(handle)
        memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
        utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
    name = raw_name.decode("utf-8", errors="replace") if isinstance(raw_name, bytes) else str(raw_name)
    return {
        "gpu_device_index": int(gpu_index),
        "gpu_name": name,
        "gpu_memory_used_bytes": int(memory.used),
        "gpu_utilization_pct": float(utilization.gpu),
    }


@dataclass
class _PhaseAggregate:
    phase: str
    started_at: str = field(default_factory=_utc_now)
    started_monotonic: float = field(default_factory=time.monotonic)
    sample_count: int = 0
    process_rss_peak_bytes: int = 0
    process_pss_peak_bytes: int = 0
    process_pss_complete_sample_count: int = 0
    process_vm_hwm_peak_bytes: int = 0
    gpu_memory_used_peak_bytes: int = 0
    gpu_process_memory_peak_bytes: int = 0
    gpu_utilization_sum_pct: float = 0.0
    gpu_utilization_peak_pct: float = 0.0
    gpu_utilization_sample_count: int = 0
    cuda_allocated_peak_bytes: int = 0
    cuda_reserved_peak_bytes: int = 0
    gpu_device_index: int | None = None
    gpu_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def observe(self, sample: dict[str, Any]) -> None:
        self.sample_count += 1
        sample_errors = sample.get("resource_sample_errors") or []
        if sample_errors:
            recorded = self.metadata.setdefault("sample_errors", [])
            error_counts = self.metadata.setdefault("sample_error_counts", {})
            for error in sample_errors:
                error_counts[str(error)] = int(error_counts.get(str(error)) or 0) + 1
                if error not in recorded and len(recorded) < 20:
                    recorded.append(error)
        self.process_rss_peak_bytes = max(self.process_rss_peak_bytes, int(sample.get("process_rss_bytes") or 0))
        if sample.get("process_pss_complete") and sample.get("process_pss_bytes") is not None:
            self.process_pss_peak_bytes = max(
                self.process_pss_peak_bytes,
                int(sample["process_pss_bytes"]),
            )
            self.process_pss_complete_sample_count += 1
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
        if sample.get("gpu_device_sample_available") is True and sample.get("gpu_utilization_pct") is not None:
            utilization = float(sample["gpu_utilization_pct"])
            self.gpu_utilization_sum_pct += utilization
            self.gpu_utilization_peak_pct = max(self.gpu_utilization_peak_pct, utilization)
            self.gpu_utilization_sample_count += 1
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
        for component in ("device", "process"):
            availability_key = f"gpu_{component}_sample_available"
            if sample.get(availability_key) is True:
                count_key = f"gpu_{component}_sample_success_count"
                self.metadata[count_key] = int(self.metadata.get(count_key) or 0) + 1
            source = sample.get(f"gpu_{component}_sample_source")
            if source:
                source_counts = self.metadata.setdefault(f"gpu_{component}_sample_source_counts", {})
                source_counts[str(source)] = int(source_counts.get(str(source)) or 0) + 1
        if sample.get("gpu_device_sample_cache_hit") is True:
            self.metadata["gpu_device_cache_hit_count"] = int(
                self.metadata.get("gpu_device_cache_hit_count") or 0
            ) + 1
        elif sample.get("gpu_device_sample_cache_hit") is False:
            self.metadata["gpu_device_cache_refresh_count"] = int(
                self.metadata.get("gpu_device_cache_refresh_count") or 0
            ) + 1

    def event_fields(self) -> dict[str, Any]:
        ended_at = _utc_now()
        metadata = dict(self.metadata)
        metadata.update(
            {
                "process_rss_semantics": (
                    "sum_of_process_tree_rss; shared_pages_may_be_counted_once_per_process"
                ),
                "process_pss_semantics": (
                    "sum_of_process_tree_pss; shared_pages_are_proportionally_apportioned"
                ),
                "process_pss_peak_bytes": self.process_pss_peak_bytes or None,
                "process_pss_complete_sample_count": self.process_pss_complete_sample_count,
                "process_capacity_metric": (
                    "process_pss_peak_bytes" if self.process_pss_complete_sample_count else None
                ),
                "gpu_utilization_sample_count": self.gpu_utilization_sample_count,
            }
        )
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
                self.gpu_utilization_sum_pct / self.gpu_utilization_sample_count
                if self.gpu_utilization_sample_count
                else None
            ),
            "gpu_utilization_peak_pct": (
                self.gpu_utilization_peak_pct if self.gpu_utilization_sample_count else None
            ),
            "cuda_allocated_peak_bytes": self.cuda_allocated_peak_bytes or None,
            "cuda_reserved_peak_bytes": self.cuda_reserved_peak_bytes or None,
            "metadata": metadata,
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
        self.gpu_cache_max_age = _env_float(
            "QE_RESOURCE_GPU_CACHE_MAX_AGE_SEC",
            self.sample_interval,
        )
        cache_root = _env("QE_RESOURCE_GPU_CACHE_DIR")
        self.gpu_cache_dir = Path(cache_root) if cache_root else Path(tempfile.gettempdir()) / "aistock-qe-gpu"
        self.wsl_runtime = _is_wsl_runtime()
        self.upload_timeout = _env_float("QE_RESOURCE_UPLOAD_TIMEOUT_SEC", 10.0)
        self.upload_retry_interval = _env_float("QE_RESOURCE_UPLOAD_RETRY_INTERVAL_SEC", 5.0)
        self.final_upload_grace = _env_float("QE_RESOURCE_FINAL_UPLOAD_GRACE_SEC", 30.0)
        self.url = self._resolve_url()
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._phase = _PhaseAggregate("bootstrap")
        self._sequence_no = 0
        self._last_uploaded_sequence_no = 0
        self._events: list[dict[str, Any]] = []
        self._upload_broken = False
        self._next_upload_retry_monotonic = 0.0
        self._sample_error_counts: dict[str, int] = {}
        self._sample_error_last_print: dict[str, float] = {}
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
        if self.wsl_runtime:
            print(
                f"[WARN] reason_code={_WSL_GPU_QUERY_SUPPRESSED_REASON} "
                "device_metrics_source=unavailable process_metrics_source=torch.cuda "
                "node_gpu_metrics_must_be_collected_by_the_windows_host"
            )
        self._sample_once()
        self._thread = threading.Thread(target=self._sample_loop, name="qe-resource-sampler", daemon=True)
        self._thread.start()
        self._write_local()

    def _sample_loop(self) -> None:
        while not self._stop.wait(self.sample_interval):
            self._sample_once()
            self._retry_pending_uploads_if_due()

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
        errors: list[str] = []

        try:
            sample.update(self._gpu_device_sample())
        except Exception as exc:
            error = f"QE_RESOURCE_GPU_SAMPLE_FAILED:device:{type(exc).__name__}"
            errors.append(error)
            sample["gpu_device_sample_available"] = False
            sample["gpu_device_sample_source"] = "pynvml_shared_cache"
            self._print_sample_error(
                reason_code="QE_RESOURCE_GPU_SAMPLE_FAILED",
                component="device",
                error=exc,
            )

        try:
            torch_sample = self._torch_sample()
            sample.update(torch_sample)
            if torch_sample.get("cuda_reserved_bytes") is not None:
                sample["gpu_process_memory_bytes"] = int(torch_sample["cuda_reserved_bytes"])
                sample["gpu_process_sample_available"] = True
                sample["gpu_process_sample_source"] = "torch.cuda.memory_reserved"
            else:
                sample["gpu_process_sample_available"] = False
                sample["gpu_process_sample_source"] = str(
                    torch_sample.get("gpu_process_sample_source") or "torch_cuda_not_initialized"
                )
        except Exception as exc:
            error = f"QE_RESOURCE_CUDA_SAMPLE_FAILED:{type(exc).__name__}"
            errors.append(error)
            sample["gpu_process_sample_available"] = False
            sample["gpu_process_sample_source"] = "torch_cuda_error"
            self._print_sample_error(
                reason_code="QE_RESOURCE_CUDA_SAMPLE_FAILED",
                component="process",
                error=exc,
            )
        if errors:
            sample["resource_sample_errors"] = errors
        return sample

    def _print_sample_error(self, *, reason_code: str, component: str, error: Exception) -> None:
        key = f"{reason_code}:{component}:{type(error).__name__}"
        count = int(self._sample_error_counts.get(key) or 0) + 1
        self._sample_error_counts[key] = count
        now = time.monotonic()
        last_print = float(self._sample_error_last_print.get(key) or 0.0)
        if count == 1 or now - last_print >= 60.0:
            self._sample_error_last_print[key] = now
            print(
                f"[ERROR] reason_code={reason_code} component={component} "
                f"error={type(error).__name__}: {error} occurrence_count={count}"
            )

    @staticmethod
    def _read_vm_hwm(pid: int) -> int:
        try:
            for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            return 0
        return 0

    @staticmethod
    def _read_pss(pid: int) -> int | None:
        try:
            for line in Path(f"/proc/{pid}/smaps_rollup").read_text(
                encoding="utf-8",
                errors="replace",
            ).splitlines():
                if line.startswith("Pss:"):
                    return int(line.split()[1]) * 1024
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
            return None
        return None

    def _process_sample(self) -> dict[str, Any]:
        pid = os.getpid()
        if psutil is None:
            pss = self._read_pss(pid)
            return {
                "process_rss_bytes": 0,
                "process_pss_bytes": pss,
                "process_pss_complete": pss is not None,
                "process_vm_hwm_bytes": self._read_vm_hwm(pid),
                "process_pids": [pid],
                "process_sampler": "proc_current_only",
            }
        root = psutil.Process(pid)
        processes = [root, *root.children(recursive=True)]
        rss = 0
        pss = 0
        pss_sampled_pids = 0
        hwm = 0
        pids: list[int] = []
        for process in processes:
            try:
                pids.append(process.pid)
                rss += int(process.memory_info().rss)
                hwm = max(hwm, self._read_vm_hwm(process.pid))
                process_pss = self._read_pss(process.pid)
                if process_pss is not None:
                    pss += process_pss
                    pss_sampled_pids += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return {
            "process_rss_bytes": rss,
            "process_pss_bytes": pss if pss_sampled_pids else None,
            "process_pss_complete": bool(pids) and pss_sampled_pids == len(pids),
            "process_vm_hwm_bytes": hwm,
            "process_pids": pids,
            "process_sampler": "psutil_process_tree",
        }

    def _gpu_cache_paths(self, gpu_index: int) -> tuple[Path, Path]:
        identity = f"{_safe_cache_component(self.node_id)}-gpu{int(gpu_index)}"
        return (
            self.gpu_cache_dir / f"{identity}.json",
            self.gpu_cache_dir / f"{identity}.lock",
        )

    def _read_fresh_gpu_cache(self, path: Path, *, gpu_index: int, now: float) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            sampled_at = float(payload["sampled_at_epoch"])
            if payload.get("schema_version") != _GPU_CACHE_SCHEMA_VERSION:
                raise ValueError("schema_version mismatch")
            if str(payload.get("node_id") or "") != self.node_id:
                raise ValueError("node_id mismatch")
            if int(payload.get("gpu_device_index")) != int(gpu_index):
                raise ValueError("gpu_device_index mismatch")
            age = max(0.0, now - sampled_at)
            if age > self.gpu_cache_max_age:
                return None
            payload["gpu_device_sample_cache_hit"] = True
            payload["gpu_device_sample_age_seconds"] = age
            return payload
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            print(
                "[ERROR] reason_code=QE_RESOURCE_GPU_CACHE_INVALID "
                f"path={path} error={type(exc).__name__}: {exc}"
            )
            return None

    def _query_nvml_device(self, gpu_index: int) -> dict[str, Any]:
        return _nvml_device_snapshot(gpu_index)

    def _gpu_device_sample(self) -> dict[str, Any]:
        if self.wsl_runtime:
            return {
                "gpu_device_sample_available": False,
                "gpu_device_sample_source": "wsl_torch_only",
                "gpu_device_sample_skipped_reason_code": _WSL_GPU_QUERY_SUPPRESSED_REASON,
            }
        if self.phase_pipeline_enabled and self._phase.phase in {"backtest", "finalize"}:
            return {
                "gpu_device_sample_available": False,
                "gpu_device_sample_source": "phase_not_gpu",
                "gpu_device_sample_skipped_phase": self._phase.phase,
            }
        gpu_index = int(_env("QE_GPU_DEVICE_INDEX") or 0)
        cache_path, lock_path = self._gpu_cache_paths(gpu_index)
        with _exclusive_file_lock(lock_path):
            now = time.time()
            cached = self._read_fresh_gpu_cache(cache_path, gpu_index=gpu_index, now=now)
            if cached is not None:
                return cached
            snapshot = dict(self._query_nvml_device(gpu_index))
            snapshot.update(
                {
                    "schema_version": _GPU_CACHE_SCHEMA_VERSION,
                    "node_id": self.node_id,
                    "sampled_at_epoch": now,
                    "gpu_device_sample_available": True,
                    "gpu_device_sample_source": "pynvml_shared_cache",
                    "gpu_device_sample_cache_hit": False,
                    "gpu_device_sample_age_seconds": 0.0,
                    "gpu_device_sample_owner_pid": os.getpid(),
                }
            )
            _atomic_json(cache_path, snapshot)
            return snapshot

    @staticmethod
    def _torch_sample() -> dict[str, Any]:
        torch = sys.modules.get("torch")
        if torch is None or not getattr(torch, "cuda", None):
            return {"gpu_process_sample_source": "torch_not_loaded"}
        is_initialized = getattr(torch.cuda, "is_initialized", None)
        if not callable(is_initialized) or not is_initialized():
            return {"gpu_process_sample_source": "torch_cuda_not_initialized"}
        return {
            "cuda_allocated_bytes": int(torch.cuda.memory_allocated()),
            "cuda_reserved_bytes": int(torch.cuda.memory_reserved()),
            "gpu_process_sample_source": "torch.cuda.memory_reserved",
        }

    def transition(self, next_phase: str, *, metadata: dict[str, Any] | None = None) -> None:
        with self._lock:
            if self._finished:
                return
            if next_phase == self._phase.phase:
                self._phase.metadata.update(dict(metadata or {}))
                self._write_local()
                print(
                    "[INFO] reason_code=QE_RESOURCE_PHASE_ALREADY_ACTIVE "
                    f"phase={next_phase} transition_skipped=true"
                )
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

    def release_gpu_phase(self, *, proof: dict[str, Any], next_phase: str = "backtest") -> bool:
        with self._lock:
            if next_phase not in _GPU_RELEASE_NEXT_PHASES:
                raise ValueError(
                    "QE GPU phase release next_phase must be one of "
                    f"{sorted(_GPU_RELEASE_NEXT_PHASES)}, got {next_phase!r}"
                )
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
            self._phase = _PhaseAggregate(next_phase)
            self._sample_once()
            return passed and self._last_uploaded_sequence_no >= int(release_event["sequence_no"])

    def last_gpu_release_event(self) -> dict[str, Any] | None:
        with self._lock:
            for event in reversed(self._events):
                if event.get("phase") in {"gpu_phase_released", "release_rejected"}:
                    return dict(event)
        return None

    def event_is_uploaded(self, sequence_no: int) -> bool:
        with self._lock:
            return self._last_uploaded_sequence_no >= int(sequence_no)

    def finish(self, *, status: str, error: str | None = None) -> None:
        with self._lock:
            if self._finished:
                return
            terminal = "completed" if status == "completed" else "failed"
            if error:
                self._phase.metadata["terminal_error_type"] = error
            self._publish_current(terminal)
            terminal_event = self._base_event(terminal, terminal)
            terminal_event["reason_code"] = (
                "QE_RESOURCE_RUN_COMPLETED" if terminal == "completed" else "QE_RESOURCE_RUN_FAILED"
            )
            if error:
                terminal_event["metadata"] = {"error_type": error}
            self._publish(terminal_event)
            self._finished = True
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                print("[ERROR] reason_code=QE_RESOURCE_SAMPLER_STOP_TIMEOUT")
        deadline = time.monotonic() + self.final_upload_grace
        while self._last_uploaded_sequence_no < self._sequence_no and time.monotonic() < deadline:
            retry_wait = self._next_upload_retry_monotonic - time.monotonic()
            if retry_wait > 0:
                time.sleep(min(retry_wait, max(0.0, deadline - time.monotonic())))
                continue
            with self._lock:
                self._flush_pending_events()
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
        queued = dict(event)
        expected_sequence = self._sequence_no + 1
        if int(queued.get("sequence_no") or 0) != expected_sequence:
            raise RuntimeError(
                f"QE resource outbox sequence mismatch: expected={expected_sequence}, "
                f"actual={queued.get('sequence_no')}"
            )
        self._sequence_no = expected_sequence
        self._events.append(queued)
        self._write_local()
        if self._upload_broken and time.monotonic() < self._next_upload_retry_monotonic:
            return
        self._flush_pending_events()

    def _flush_pending_events(self) -> bool:
        pending = [
            event
            for event in self._events
            if int(event.get("sequence_no") or 0) > self._last_uploaded_sequence_no
        ]
        if not pending:
            self._upload_broken = False
            return True

        was_broken = self._upload_broken
        for event in pending:
            error: Exception | None = None
            for attempt in range(1, 4):
                try:
                    response = requests.post(self.url, json=event, headers={"X-QE-Resource-Token": self.token}, timeout=self.upload_timeout)
                    if response.status_code >= 400:
                        raise RuntimeError(f"HTTP_{response.status_code}")
                    self._last_uploaded_sequence_no = int(event["sequence_no"])
                    self._write_local()
                    error = None
                    break
                except Exception as exc:  # retry the exact payload for server idempotency
                    error = exc
                    if attempt < 3:
                        time.sleep(min(2.0, 0.25 * (2 ** (attempt - 1))))
            if error is not None:
                self._record_upload_failure(event, error)
                return False

        self._upload_broken = False
        self._next_upload_retry_monotonic = 0.0
        if was_broken:
            recovery = {
                "schema_version": "qe_runtime_resource_upload_failure_v1",
                "reason_code": "QE_RESOURCE_EVENT_UPLOAD_RECOVERED",
                "failure_reason_code": "QE_RESOURCE_EVENT_UPLOAD_FAILED",
                "session_id": self.session_id,
                "status": "recovered",
                "last_uploaded_sequence_no": self._last_uploaded_sequence_no,
                "recovered_at": _utc_now(),
            }
            _atomic_json(Path.cwd() / UPLOAD_FAILURE_FILE, recovery)
            print("[INFO] reason_code=QE_RESOURCE_EVENT_UPLOAD_RECOVERED")
        self._write_local()
        return True

    def _record_upload_failure(self, event: dict[str, Any], error: Exception) -> None:
        self._upload_broken = True
        self._next_upload_retry_monotonic = time.monotonic() + self.upload_retry_interval
        error_type = type(error).__name__
        marker = {
            "schema_version": "qe_runtime_resource_upload_failure_v1",
            "reason_code": "QE_RESOURCE_EVENT_UPLOAD_FAILED",
            "session_id": self.session_id,
            "sequence_no": event.get("sequence_no"),
            "phase": event.get("phase"),
            "error_type": error_type,
            "retry_attempts": 3,
            "written_at": _utc_now(),
        }
        _atomic_json(Path.cwd() / UPLOAD_FAILURE_FILE, marker)
        self._write_local()
        print("[ERROR] reason_code=QE_RESOURCE_EVENT_UPLOAD_FAILED")

    def _retry_pending_uploads_if_due(self) -> None:
        with self._lock:
            if self._last_uploaded_sequence_no >= self._sequence_no:
                return
            if time.monotonic() < self._next_upload_retry_monotonic:
                return
            self._flush_pending_events()

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
            "last_uploaded_sequence_no": self._last_uploaded_sequence_no,
            "pending_event_count": max(0, self._sequence_no - self._last_uploaded_sequence_no),
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


def resource_phase_pipeline_active() -> bool:
    return bool(_MONITOR is not None and _MONITOR.phase_pipeline_enabled)


def _resource_phase_events_deferred() -> bool:
    return int(getattr(_PHASE_EVENT_STATE, "defer_depth", 0) or 0) > 0


@contextmanager
def defer_resource_phase_events(reason: str):
    """Keep a cyclic inner workflow under one outer monotonic QE phase session."""

    if not resource_phase_pipeline_active():
        yield
        return
    previous_depth = int(getattr(_PHASE_EVENT_STATE, "defer_depth", 0) or 0)
    _PHASE_EVENT_STATE.defer_depth = previous_depth + 1
    _PHASE_EVENT_STATE.defer_reason = str(reason or "unspecified")
    print(
        "[INFO] reason_code=QE_RESOURCE_PHASE_EVENTS_DEFERRED "
        f"reason={_PHASE_EVENT_STATE.defer_reason} depth={previous_depth + 1}"
    )
    try:
        yield
    finally:
        _PHASE_EVENT_STATE.defer_depth = previous_depth
        if previous_depth == 0:
            _PHASE_EVENT_STATE.defer_reason = None


def transition_resource_phase(phase: str, *, metadata: dict[str, Any] | None = None) -> None:
    if _resource_phase_events_deferred():
        print(
            "[INFO] reason_code=QE_RESOURCE_PHASE_EVENT_DEFERRED "
            f"phase={phase} reason={getattr(_PHASE_EVENT_STATE, 'defer_reason', None)}"
        )
        return
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


def publish_gpu_phase_release(proof: dict[str, Any], *, next_phase: str = "backtest") -> bool:
    if _resource_phase_events_deferred():
        print(
            "[INFO] reason_code=QE_GPU_PHASE_RELEASE_DEFERRED "
            f"reason={getattr(_PHASE_EVENT_STATE, 'defer_reason', None)}"
        )
        return False
    if _MONITOR is None:
        if _env_bool("QE_PHASE_PIPELINE_ENABLED", False):
            print("[ERROR] reason_code=QE_GPU_PHASE_HELPER_MISSING release_event_not_published=true")
        return False
    return _MONITOR.release_gpu_phase(proof=proof, next_phase=next_phase)


def _last_gpu_release_event() -> dict[str, Any] | None:
    if _MONITOR is None:
        return None
    return _MONITOR.last_gpu_release_event()


def _gpu_release_tolerance_bytes() -> int:
    raw = _env("QE_GPU_PHASE_RELEASE_TOLERANCE_BYTES")
    value = int(raw) if raw else _DEFAULT_GPU_PHASE_RELEASE_TOLERANCE_BYTES
    if value < 0:
        raise ValueError("QE_GPU_PHASE_RELEASE_TOLERANCE_BYTES must be non-negative")
    return value


def _torch_cuda_release_snapshot() -> dict[str, Any]:
    torch = sys.modules.get("torch")
    if torch is None or not getattr(torch, "cuda", None):
        return {
            "cuda_allocated_bytes": 0,
            "cuda_reserved_bytes": 0,
            "release_snapshot_source": "torch_not_loaded",
        }
    is_initialized = getattr(torch.cuda, "is_initialized", None)
    if not callable(is_initialized) or not is_initialized():
        return {
            "cuda_allocated_bytes": 0,
            "cuda_reserved_bytes": 0,
            "release_snapshot_source": "torch_cuda_not_initialized",
        }
    torch.cuda.synchronize()
    gc.collect()
    torch.cuda.empty_cache()
    return {
        "cuda_allocated_bytes": int(torch.cuda.memory_allocated()),
        "cuda_reserved_bytes": int(torch.cuda.memory_reserved()),
        "release_snapshot_source": "torch_cuda_in_process",
    }


def capture_gpu_phase_release_baseline() -> dict[str, Any]:
    if not resource_phase_pipeline_active():
        return {
            "release_baseline_allocated_bytes": 0,
            "release_baseline_reserved_bytes": 0,
            "release_capture_source": "phase_pipeline_inactive",
        }
    try:
        snapshot = _torch_cuda_release_snapshot()
        return {
            "release_baseline_allocated_bytes": int(snapshot["cuda_allocated_bytes"]),
            "release_baseline_reserved_bytes": int(snapshot["cuda_reserved_bytes"]),
            "release_capture_source": snapshot["release_snapshot_source"],
        }
    except Exception as exc:
        print(
            "[ERROR] reason_code=QE_GPU_PHASE_RELEASE_BASELINE_FAILED "
            f"error={type(exc).__name__}"
        )
        return {
            "release_baseline_allocated_bytes": 0,
            "release_baseline_reserved_bytes": 0,
            "release_capture_source": "capture_failed",
            "release_capture_error_type": type(exc).__name__,
        }


def finalize_gpu_phase_release(
    baseline: dict[str, Any] | None,
    *,
    predict_error: BaseException | None = None,
    next_phase: str = "backtest",
) -> bool:
    if not resource_phase_pipeline_active():
        return False
    existing = _last_gpu_release_event()
    if existing is not None:
        sequence_no = int(existing.get("sequence_no") or 0)
        acknowledged = bool(
            existing.get("phase") == "gpu_phase_released"
            and _MONITOR is not None
            and _MONITOR.event_is_uploaded(sequence_no)
        )
        print(
            "[INFO] reason_code=QE_GPU_PHASE_RELEASE_ALREADY_PUBLISHED "
            f"phase={existing.get('phase')} acknowledged={str(acknowledged).lower()}"
        )
        return acknowledged

    baseline = dict(baseline or {})
    tolerance = _gpu_release_tolerance_bytes()
    release_error_type = None
    try:
        after = _torch_cuda_release_snapshot()
    except Exception as exc:
        release_error_type = type(exc).__name__
        print(
            "[ERROR] reason_code=QE_GPU_PHASE_RELEASE_SNAPSHOT_FAILED "
            f"error={release_error_type}"
        )
        after = {
            "cuda_allocated_bytes": int(baseline.get("release_baseline_allocated_bytes") or 0),
            "cuda_reserved_bytes": int(baseline.get("release_baseline_reserved_bytes") or 0),
            "release_snapshot_source": "snapshot_failed",
        }

    allocated_after = int(after["cuda_allocated_bytes"])
    reserved_after = int(after["cuda_reserved_bytes"])
    allocated_limit = int(baseline.get("release_baseline_allocated_bytes") or 0) + tolerance
    reserved_limit = int(baseline.get("release_baseline_reserved_bytes") or 0) + tolerance
    release_check_passed = bool(
        predict_error is None
        and release_error_type is None
        and not baseline.get("release_capture_error_type")
        and allocated_after <= allocated_limit
        and reserved_after <= reserved_limit
    )
    proof = {
        **baseline,
        "cuda_allocated_bytes_after": allocated_after,
        "cuda_reserved_bytes_after": reserved_after,
        "release_tolerance_bytes": tolerance,
        "release_check_passed": release_check_passed,
        "release_proof_source": "runner_generic_cuda_baseline_v1",
        "release_snapshot_source": after.get("release_snapshot_source"),
    }
    if predict_error is not None:
        proof["predict_error_type"] = type(predict_error).__name__
    if release_error_type is not None:
        proof["release_snapshot_error_type"] = release_error_type
    return publish_gpu_phase_release(proof, next_phase=next_phase)


def _record_is_portfolio_backtest(record: Any) -> bool:
    record_class = record.get("class") if isinstance(record, dict) else record
    if isinstance(record_class, str):
        name = record_class
    else:
        name = getattr(record_class, "__name__", type(record_class).__name__)
    return "PortAna" in name


def task_train_with_resource_phases(
    task_config: dict[str, Any],
    *,
    experiment_name: str,
    recorder_name: str | None = None,
    release_next_phase: str = "backtest",
):
    """Run Qlib's task_train contract with real QE train/predict/backtest boundaries."""

    import qlib.model.trainer as trainer

    if not resource_phase_pipeline_active() or _resource_phase_events_deferred():
        return trainer.task_train(
            task_config,
            experiment_name=experiment_name,
            recorder_name=recorder_name,
        )

    with trainer.R.start(experiment_name=experiment_name, recorder_name=recorder_name):
        trainer._log_task_info(task_config)
        recorder = trainer.R.get_recorder()
        model = trainer.init_instance_by_config(task_config["model"], accept_types=trainer.Model)
        dataset = trainer.init_instance_by_config(task_config["dataset"], accept_types=trainer.Dataset)
        reweighter = task_config.get("reweighter", None)

        transition_resource_phase("train", metadata={"phase_source": "qlib_task_train"})
        trainer.auto_filter_kwargs(model.fit)(dataset, reweighter=reweighter)
        trainer.R.save_objects(**{"params.pkl": model})
        dataset.config(dump_all=False, recursive=True)
        trainer.R.save_objects(**{"dataset": dataset})

        placeholder_value = {"<MODEL>": model, "<DATASET>": dataset}
        filled_task_config = trainer.fill_placeholder(task_config, placeholder_value)
        records = filled_task_config.get("record", [])
        if isinstance(records, dict):
            records = [records]
        first_backtest_index = next(
            (index for index, record in enumerate(records) if _record_is_portfolio_backtest(record)),
            None,
        )

        transition_resource_phase("predict", metadata={"phase_source": "qlib_task_records"})
        release_baseline = capture_gpu_phase_release_baseline()
        release_attempted = False
        try:
            for index, record in enumerate(records):
                if not release_attempted and index == first_backtest_index:
                    finalize_gpu_phase_release(release_baseline, next_phase="backtest")
                    release_attempted = True
                record_instance = trainer.init_instance_by_config(
                    record,
                    recorder=recorder,
                    default_module="qlib.workflow.record_temp",
                    try_kwargs={"model": model, "dataset": dataset},
                )
                record_instance.generate()
            if not release_attempted:
                finalize_gpu_phase_release(release_baseline, next_phase=release_next_phase)
        except Exception as exc:
            if not release_attempted and _last_gpu_release_event() is None:
                finalize_gpu_phase_release(
                    release_baseline,
                    predict_error=exc,
                    next_phase="finalize",
                )
            raise
        return trainer.R.get_recorder()


def finish_resource_monitor(*, status: str, error: str | None = None) -> None:
    if _MONITOR is not None:
        _MONITOR.finish(status=status, error=error)
