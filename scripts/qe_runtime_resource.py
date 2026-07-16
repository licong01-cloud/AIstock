"""Runner-side QE phase publisher with all resource monitoring disabled.

The filename is retained because existing workspace composition copies it by
name.  It deliberately contains no GPU, VRAM, CUDA allocator, CPU, RSS, PSS,
NVML, or subprocess resource probes.  The only remaining responsibility is to
publish authenticated lifecycle events so model-policy concurrency and
train/predict/backtest sequencing continue to work without resource gates.
"""

from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


PHASE_FILE = "qe_runtime_phase.json"
UPLOAD_FAILURE_FILE = "qe_runtime_phase_upload_failure.json"
RESOURCE_SECRET_FILE = "qe_resource_session_secret.json"
BASE_URL_ENVS = (
    "AISTOCK_PREDICTION_STORE_BASE_URL",
    "AISTOCK_QE_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_CALLBACK_BASE_URL",
    "AISTOCK_BACKEND_BASE_URL",
)
RESOURCE_MONITORING_DISABLED_REASON = "QE_RESOURCE_MONITORING_DISABLED"
GPU_PHASE_LIFECYCLE_REASON = "QE_GPU_PHASE_LIFECYCLE_COMPLETE"
_GPU_RELEASE_NEXT_PHASES = {"backtest", "finalize"}
_PHASE_EVENT_STATE = threading.local()
_PUBLISHER: "QERuntimePhasePublisher | None" = None
_RESOURCE_SECRET_CACHE: dict[str, Any] | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


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


@dataclass
class _PhaseState:
    phase: str
    started_at: str = field(default_factory=_utc_now)
    started_monotonic: float = field(default_factory=time.monotonic)
    metadata: dict[str, Any] = field(default_factory=dict)

    def event_fields(self) -> dict[str, Any]:
        metadata = {
            **self.metadata,
            "resource_monitoring_enabled": False,
            "resource_monitoring_reason_code": RESOURCE_MONITORING_DISABLED_REASON,
        }
        return {
            "started_at": self.started_at,
            "ended_at": _utc_now(),
            "duration_seconds": max(0.0, time.monotonic() - self.started_monotonic),
            "metadata": metadata,
        }


class QERuntimePhasePublisher:
    """Authenticated lifecycle publisher; never samples machine resources."""

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
        self.upload_timeout = _env_float("QE_RESOURCE_UPLOAD_TIMEOUT_SEC", 10.0)
        self.upload_retry_interval = _env_float("QE_RESOURCE_UPLOAD_RETRY_INTERVAL_SEC", 5.0)
        self.final_upload_grace = _env_float("QE_RESOURCE_FINAL_UPLOAD_GRACE_SEC", 30.0)
        self.url = self._resolve_url()
        self._lock = threading.RLock()
        self._phase = _PhaseState("bootstrap")
        self._sequence_no = 0
        self._last_uploaded_sequence_no = 0
        self._events: list[dict[str, Any]] = []
        self._upload_broken = False
        self._next_upload_retry_monotonic = 0.0
        self._finished = False

    @classmethod
    def from_env(cls) -> "QERuntimePhasePublisher | None":
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
                    "[ERROR] reason_code=QE_PHASE_PUBLISHER_MISSING "
                    f"missing_env={missing}; phase pipeline cannot publish lifecycle events"
                )
            return None
        return cls()

    def _resolve_url(self) -> str:
        base = next((_env(name) for name in BASE_URL_ENVS if _env(name)), "")
        if not base:
            raise RuntimeError("QE_PHASE_EVENT_UPLOAD_FAILED: no backend base URL configured")
        parsed = urlparse(base)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise RuntimeError(f"QE phase base must be absolute http(s), got {base!r}")
        base = base.rstrip("/")
        path = "/api/v1/quantevolver/evolution/webhook/loop-resource-phase"
        if base.endswith("/api/v1"):
            return f"{base}{path.removeprefix('/api/v1')}"
        return f"{base}{path}"

    def start(self) -> None:
        print(
            f"[INFO] reason_code={RESOURCE_MONITORING_DISABLED_REASON} "
            "phase_events_only=true gpu_queries=false process_resource_queries=false"
        )
        self._write_local()

    def transition(self, next_phase: str, *, metadata: dict[str, Any] | None = None) -> None:
        with self._lock:
            if self._finished:
                return
            if next_phase == self._phase.phase:
                self._phase.metadata.update(dict(metadata or {}))
                self._write_local()
                print(
                    "[INFO] reason_code=QE_PHASE_ALREADY_ACTIVE "
                    f"phase={next_phase} transition_skipped=true"
                )
                return
            self._publish_current("completed")
            self._phase = _PhaseState(next_phase, metadata=dict(metadata or {}))
            self._write_local()

    def record_model_resident_state(
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

    def publish_gpu_phase_complete(self, *, next_phase: str = "backtest") -> bool:
        with self._lock:
            if next_phase not in _GPU_RELEASE_NEXT_PHASES:
                raise ValueError(
                    "QE GPU phase lifecycle next_phase must be one of "
                    f"{sorted(_GPU_RELEASE_NEXT_PHASES)}, got {next_phase!r}"
                )
            self._publish_current("completed")
            event = self._base_event("gpu_phase_released", "released")
            event["release_check_passed"] = None
            event["reason_code"] = GPU_PHASE_LIFECYCLE_REASON
            event["metadata"] = {
                "resource_monitoring_enabled": False,
                "resource_monitoring_reason_code": RESOURCE_MONITORING_DISABLED_REASON,
                "release_semantics": "predict_lifecycle_completed",
            }
            self._publish(event)
            self._phase = _PhaseState(next_phase)
            self._write_local()
            return self._last_uploaded_sequence_no >= int(event["sequence_no"])

    def last_gpu_phase_event(self) -> dict[str, Any] | None:
        with self._lock:
            for event in reversed(self._events):
                if event.get("phase") == "gpu_phase_released":
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
            event = self._base_event(terminal, terminal)
            event["reason_code"] = "QE_PHASE_RUN_COMPLETED" if terminal == "completed" else "QE_PHASE_RUN_FAILED"
            event["metadata"] = {
                "resource_monitoring_enabled": False,
                "resource_monitoring_reason_code": RESOURCE_MONITORING_DISABLED_REASON,
                **({"error_type": error} if error else {}),
            }
            self._publish(event)
            self._finished = True
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
        fields = self._phase.event_fields()
        event = self._base_event(self._phase.phase, phase_status)
        event.update(fields)
        resident = fields.get("metadata") or {}
        for key in ("resident_requested", "resident_active", "resident_fallback", "fallback_reason_code"):
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
                f"QE phase outbox sequence mismatch: expected={expected_sequence}, actual={queued.get('sequence_no')}"
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
                    response = requests.post(
                        self.url,
                        json=event,
                        headers={"X-QE-Resource-Token": self.token},
                        timeout=self.upload_timeout,
                    )
                    if response.status_code >= 400:
                        raise RuntimeError(f"HTTP_{response.status_code}")
                    self._last_uploaded_sequence_no = int(event["sequence_no"])
                    self._write_local()
                    error = None
                    break
                except Exception as exc:  # exact payload retry preserves server idempotency
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
                "schema_version": "qe_runtime_phase_upload_failure_v1",
                "reason_code": "QE_PHASE_EVENT_UPLOAD_RECOVERED",
                "session_id": self.session_id,
                "status": "recovered",
                "last_uploaded_sequence_no": self._last_uploaded_sequence_no,
                "recovered_at": _utc_now(),
            }
            _atomic_json(Path.cwd() / UPLOAD_FAILURE_FILE, recovery)
            print("[INFO] reason_code=QE_PHASE_EVENT_UPLOAD_RECOVERED")
        self._write_local()
        return True

    def _record_upload_failure(self, event: dict[str, Any], error: Exception) -> None:
        self._upload_broken = True
        self._next_upload_retry_monotonic = time.monotonic() + self.upload_retry_interval
        marker = {
            "schema_version": "qe_runtime_phase_upload_failure_v1",
            "reason_code": "QE_PHASE_EVENT_UPLOAD_FAILED",
            "session_id": self.session_id,
            "sequence_no": event.get("sequence_no"),
            "phase": event.get("phase"),
            "error_type": type(error).__name__,
            "retry_attempts": 3,
            "written_at": _utc_now(),
        }
        _atomic_json(Path.cwd() / UPLOAD_FAILURE_FILE, marker)
        self._write_local()
        print("[ERROR] reason_code=QE_PHASE_EVENT_UPLOAD_FAILED")

    def _write_local(self) -> None:
        payload = {
            "schema_version": "qe_runtime_phase_v1",
            "session_id": self.session_id,
            "source_run_key": self.source_run_key,
            "task_id": self.task_id,
            "loop_id": self.loop_id,
            "loop_index": self.loop_index,
            "node_id": self.node_id,
            "phase_pipeline_enabled": self.phase_pipeline_enabled,
            "resource_monitoring_enabled": False,
            "resource_monitoring_reason_code": RESOURCE_MONITORING_DISABLED_REASON,
            "current_phase": self._phase.phase,
            "last_sequence_no": self._sequence_no,
            "last_uploaded_sequence_no": self._last_uploaded_sequence_no,
            "pending_event_count": max(0, self._sequence_no - self._last_uploaded_sequence_no),
            "upload_broken": self._upload_broken,
            "events": list(self._events),
            "updated_at": _utc_now(),
        }
        _atomic_json(Path.cwd() / PHASE_FILE, payload)


def start_phase_publisher() -> QERuntimePhasePublisher | None:
    global _PUBLISHER
    if _PUBLISHER is not None:
        return _PUBLISHER
    try:
        publisher = QERuntimePhasePublisher.from_env()
        if publisher is not None:
            publisher.start()
        _PUBLISHER = publisher
        return publisher
    except Exception as exc:
        print(f"[ERROR] reason_code=QE_PHASE_PUBLISHER_START_FAILED error={type(exc).__name__}: {exc}")
        if _env_bool("QE_PHASE_PIPELINE_ENABLED", False):
            print("[ERROR] phase pipeline lifecycle events cannot be published")
        return None


def phase_pipeline_active() -> bool:
    return bool(_PUBLISHER is not None and _PUBLISHER.phase_pipeline_enabled)


def _phase_events_deferred() -> bool:
    return int(getattr(_PHASE_EVENT_STATE, "defer_depth", 0) or 0) > 0


@contextmanager
def defer_runtime_phase_events(reason: str):
    """Keep a cyclic inner workflow under one outer monotonic QE phase session."""

    if not phase_pipeline_active():
        yield
        return
    previous_depth = int(getattr(_PHASE_EVENT_STATE, "defer_depth", 0) or 0)
    _PHASE_EVENT_STATE.defer_depth = previous_depth + 1
    _PHASE_EVENT_STATE.defer_reason = str(reason or "unspecified")
    print(
        "[INFO] reason_code=QE_PHASE_EVENTS_DEFERRED "
        f"reason={_PHASE_EVENT_STATE.defer_reason} depth={previous_depth + 1}"
    )
    try:
        yield
    finally:
        _PHASE_EVENT_STATE.defer_depth = previous_depth
        if previous_depth == 0:
            _PHASE_EVENT_STATE.defer_reason = None


def transition_runtime_phase(phase: str, *, metadata: dict[str, Any] | None = None) -> None:
    if _phase_events_deferred():
        print(
            "[INFO] reason_code=QE_PHASE_EVENT_DEFERRED "
            f"phase={phase} reason={getattr(_PHASE_EVENT_STATE, 'defer_reason', None)}"
        )
        return
    if _PUBLISHER is not None:
        _PUBLISHER.transition(phase, metadata=metadata)


def record_model_resident_state(
    *,
    requested: bool,
    active: bool,
    fallback_reason_code: str | None = None,
) -> None:
    if _PUBLISHER is not None:
        _PUBLISHER.record_model_resident_state(
            requested=requested,
            active=active,
            fallback_reason_code=fallback_reason_code,
        )


def publish_gpu_phase_complete(*, next_phase: str = "backtest") -> bool:
    if _phase_events_deferred():
        print(
            "[INFO] reason_code=QE_GPU_PHASE_LIFECYCLE_DEFERRED "
            f"reason={getattr(_PHASE_EVENT_STATE, 'defer_reason', None)}"
        )
        return False
    if _PUBLISHER is None:
        if _env_bool("QE_PHASE_PIPELINE_ENABLED", False):
            print("[ERROR] reason_code=QE_PHASE_PUBLISHER_MISSING gpu_phase_event_not_published=true")
        return False
    return _PUBLISHER.publish_gpu_phase_complete(next_phase=next_phase)


def _last_gpu_phase_event() -> dict[str, Any] | None:
    if _PUBLISHER is None:
        return None
    return _PUBLISHER.last_gpu_phase_event()


def finalize_gpu_phase_lifecycle(
    *,
    predict_error: BaseException | None = None,
    next_phase: str = "backtest",
) -> bool:
    """Publish lifecycle completion after prediction without any resource probe."""

    if not phase_pipeline_active():
        return False
    existing = _last_gpu_phase_event()
    if existing is not None:
        sequence_no = int(existing.get("sequence_no") or 0)
        acknowledged = bool(_PUBLISHER is not None and _PUBLISHER.event_is_uploaded(sequence_no))
        print(
            "[INFO] reason_code=QE_GPU_PHASE_LIFECYCLE_ALREADY_PUBLISHED "
            f"acknowledged={str(acknowledged).lower()}"
        )
        return acknowledged
    if predict_error is not None:
        print(
            "[ERROR] reason_code=QE_GPU_PHASE_LIFECYCLE_NOT_COMPLETED "
            f"predict_error={type(predict_error).__name__}"
        )
        return False
    return publish_gpu_phase_complete(next_phase=next_phase)


def _record_is_portfolio_backtest(record: Any) -> bool:
    record_class = record.get("class") if isinstance(record, dict) else record
    if isinstance(record_class, str):
        name = record_class
    else:
        name = getattr(record_class, "__name__", type(record_class).__name__)
    return "PortAna" in name


def task_train_with_phase_events(
    task_config: dict[str, Any],
    *,
    experiment_name: str,
    recorder_name: str | None = None,
    release_next_phase: str = "backtest",
):
    """Run Qlib task_train with train/predict/backtest lifecycle boundaries."""

    import qlib.model.trainer as trainer

    if not phase_pipeline_active() or _phase_events_deferred():
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

        transition_runtime_phase("train", metadata={"phase_source": "qlib_task_train"})
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

        transition_runtime_phase("predict", metadata={"phase_source": "qlib_task_records"})
        lifecycle_published = False
        try:
            for index, record in enumerate(records):
                if not lifecycle_published and index == first_backtest_index:
                    finalize_gpu_phase_lifecycle(next_phase="backtest")
                    lifecycle_published = True
                record_instance = trainer.init_instance_by_config(
                    record,
                    recorder=recorder,
                    default_module="qlib.workflow.record_temp",
                    try_kwargs={"model": model, "dataset": dataset},
                )
                record_instance.generate()
            if not lifecycle_published:
                finalize_gpu_phase_lifecycle(next_phase=release_next_phase)
        except Exception as exc:
            if not lifecycle_published and _last_gpu_phase_event() is None:
                finalize_gpu_phase_lifecycle(predict_error=exc, next_phase="finalize")
            raise
        return trainer.R.get_recorder()


def finish_phase_publisher(*, status: str, error: str | None = None) -> None:
    if _PUBLISHER is not None:
        _PUBLISHER.finish(status=status, error=error)
