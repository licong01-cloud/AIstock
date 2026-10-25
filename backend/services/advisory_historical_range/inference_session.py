"""Task-owned, bounded inference resources and compact execution timings.

No database, shared service, package admission, or mutable feature cache lives
here.  A session is explicit and may be closed independently of user services.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import logging
from pathlib import Path
import pickle
from threading import RLock
import time
from typing import Any
import sys
import weakref
import os
import tempfile

logger = logging.getLogger(__name__)


def frozen_code_identity(workspace: Path) -> str:
    roots = [workspace / "model", *(path for path in workspace.iterdir()
                                   if path.is_dir() and (path / "__init__.py").is_file())]
    files = {path for root in roots if root.is_dir() for path in root.rglob("*.py")}
    files.update(workspace.glob("*.py"))
    members = {str(path.relative_to(workspace)): hashlib.sha256(path.read_bytes()).hexdigest()
               for path in files if path.is_file()}
    return hashlib.sha256(json.dumps(members, sort_keys=True).encode()).hexdigest()


@contextmanager
def frozen_model_code_environment(workspace: Path):
    """Isolate frozen Python module names across package invocations.

    Called under the inference execution lock. Only explicitly supplied local
    code names are shadowed, and the original import environment is restored.
    No global model/processor template survives outside its own resource session.
    """
    roots = [workspace.resolve(), (workspace / "model").resolve()]
    names = {path.stem if path.is_file() else path.name
             for root in roots if root.is_dir() for path in root.iterdir()
             if (path.suffix == ".py" and path.stem != "__init__")
             or (path.is_dir() and (path / "__init__.py").is_file())}
    def owned(name: str) -> bool:
        return name.split(".", 1)[0] in names
    previous = {name: module for name, module in sys.modules.copy().items() if owned(name)}
    original_paths = list(sys.path)
    try:
        for name in previous:
            sys.modules.pop(name, None)
        sys.path[:0] = [str(root) for root in reversed(roots)]
        yield
    finally:
        for name in list(sys.modules.copy()):
            if owned(name):
                sys.modules.pop(name, None)
        sys.modules.update(previous)
        sys.path[:] = original_paths


@contextmanager
def inference_output_lease(output: Path):
    """One writer per own-run output; OS lock releases on crash, no PID control.

    The persistent one-byte lock file is not a success marker. Its path must
    remain beside the resolved output; never delete another runner's lock.
    """
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output.with_name(output.name + ".lock")
    if lock_path.resolve().parent != output.parent:
        raise ValueError("output lock escaped its owned directory")
    with lock_path.open("a+b") as stream:
        if stream.seek(0, os.SEEK_END) == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("this own-run inference output already has a writer") from exc
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


@contextmanager
def inference_temporary_environment(root: Path):
    root = root.resolve()
    on_x = root.drive.upper() == "X:" if os.name == "nt" else root.is_relative_to(Path("/mnt/x"))
    if not on_x or (os.name != "nt" and not Path("/mnt/x").is_mount()):
        raise ValueError("inference temporary storage must resolve to mounted X, never C/default tmp")
    root.mkdir(parents=True, exist_ok=True)
    paths = {"TEMP": root, "TMP": root, "TMPDIR": root}
    for variable, name in {"XDG_CACHE_HOME": "xdg", "TORCH_HOME": "torch", "TRITON_CACHE_DIR": "triton",
                           "NUMBA_CACHE_DIR": "numba", "CUDA_CACHE_PATH": "cuda", "MPLCONFIGDIR": "matplotlib"}.items():
        directory = (root / "library_cache" / name).resolve()
        if not directory.is_relative_to(root):
            raise ValueError("inference library cache escaped the owned X root")
        directory.mkdir(parents=True, exist_ok=True)
        paths[variable] = directory
    previous = {name: os.environ.get(name) for name in paths}
    previous_tempdir = tempfile.tempdir
    try:
        os.environ.update({name: str(path) for name, path in paths.items()})
        tempfile.tempdir = str(root)
        yield root
    finally:
        tempfile.tempdir = previous_tempdir
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@dataclass(frozen=True)
class FrozenInferProcessorBundle:
    step_len: Any
    processors: tuple[Any, ...]
    snapshot: bytes | None = None

    @classmethod
    def from_dataset(cls, dataset: Any) -> FrozenInferProcessorBundle:
        handler = getattr(dataset, "handler", None)
        processors = tuple(getattr(handler, "infer_processors", []) or [])
        try:
            snapshot = pickle.dumps(processors, protocol=pickle.HIGHEST_PROTOCOL)
            pickle.loads(snapshot)  # Check that the fitted subset is independently restorable.
        except Exception as exc:
            # Custom fitted processors may reject serialization with their own
            # exception types. An optional reuse optimization must not create
            # a new eligibility requirement for an otherwise loadable asset.
            logger.info("infer processor resource mode=uncached_fresh_load; subset cannot be serialized independently (%s)", type(exc).__name__)
            return cls(getattr(dataset, "step_len", 1), processors)
        return cls(getattr(dataset, "step_len", 1), (), snapshot)

    def for_call(self) -> list[Any]:
        # Restore the fitted subset, not the full training dataset. This matches
        # fresh pickle loading without requiring custom processors to support
        # deepcopy, and keeps call-local processor mutations out of the cache.
        return list(pickle.loads(self.snapshot)) if self.snapshot is not None else list(self.processors)


class InferenceResourceSession:
    """Reuse admitted resources within an explicit task, never a global cache."""

    def __init__(self, *, max_bytes: int = 512 * 1024 * 1024, max_entries: int = 4) -> None:
        if max_bytes < 0 or max_entries < 1:
            raise ValueError("invalid inference resource budget")
        self.max_bytes = max_bytes
        self.max_entries = max_entries
        self._entries: OrderedDict[str, tuple[Any, int]] = OrderedDict()
        self.cache_bytes = 0
        self.hits = 0
        self.loads = 0
        self._lock = RLock()

    @staticmethod
    def _key(path: Path, namespace: str, kind: str) -> str:
        stat = path.stat()
        payload = {
            "namespace": namespace,
            "kind": kind,
            "device": stat.st_dev,
            "inode": stat.st_ino,
            "size": stat.st_size,
            "modified_ns": stat.st_mtime_ns,
            "path_without_inode": str(path.resolve()) if not stat.st_ino else None,
        }
        # Namespace binds the admitted manifest/code; stat is a change detector
        # for those immutable assets, not a substitute for package admission.
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def load(
        self, *, path: Path, namespace: str, kind: str,
        loader: Callable[[Path], Any], size_of: Callable[[Any], int | None],
    ) -> Any:
        with self._lock:
            return self._load_locked(path=path, namespace=namespace, kind=kind, loader=loader, size_of=size_of)

    def _load_locked(
        self, *, path: Path, namespace: str, kind: str,
        loader: Callable[[Path], Any], size_of: Callable[[Any], int | None],
    ) -> Any:
        key = self._key(path, namespace, kind)
        if key in self._entries:
            self._entries.move_to_end(key)
            self.hits += 1
            return self._entries[key][0]
        value = loader(path)
        self.loads += 1
        if key != self._key(path, namespace, kind):
            raise RuntimeError("inference resource changed during load")
        size = size_of(value)
        if size is None or size > self.max_bytes:
            return value  # Usable resource, explicitly not retained by this cache.
        while self._entries and (self.cache_bytes + size > self.max_bytes or len(self._entries) >= self.max_entries):
            _, (_, removed_size) = self._entries.popitem(last=False)
            self.cache_bytes -= removed_size
        self._entries[key] = (value, size)
        self.cache_bytes += size
        return value

    @staticmethod
    def _processor_size(bundle: FrozenInferProcessorBundle) -> int | None:
        if bundle.snapshot is None:
            # An otherwise usable processor is loaded afresh rather than shared.
            return None
        return len(bundle.snapshot)

    def processors(
        self, *, path: Path, namespace: str, loader: Callable[[Path], Any],
    ) -> FrozenInferProcessorBundle:
        return self.load(
            path=path, namespace=namespace, kind="frozen_processors",
            loader=lambda source: FrozenInferProcessorBundle.from_dataset(loader(source)),
            size_of=self._processor_size,
        )

    def model(
        self, *, path: Path, namespace: str, loader: Callable[[Path], Any],
    ) -> Any:
        with self._lock:
            # Hold the resource lock until isolation completes. Otherwise a
            # concurrent eviction/close could make two borrowers mistake the
            # same removed template for separate fresh caller-owned models.
            template = self._load_locked(
                path=path, namespace=namespace, kind="frozen_model",
                loader=loader, size_of=_retained_bytes,
            )
            if not any(value is template for value, _ in self._entries.values()):
                return template  # Fresh caller-owned resource, no reuse/isolation needed.
            try:
                invocation = deepcopy(template)
                if invocation[0] is template[0]:
                    raise TypeError("model deepcopy returned shared mutable state")
                if template[2] is not None and invocation[2] is template[2]:
                    raise TypeError("model deepcopy returned a shared mutable network")
                return invocation
            except Exception as exc:
                # Package eligibility never depends on support for reuse. Use
                # the fresh loader and retain its normal asset-change check.
                logger.info("model resource mode=uncached_fresh_load; isolation unsupported (%s)", type(exc).__name__)
                key = self._key(path, namespace, "frozen_model")
                removed = self._entries.pop(key, None)
                if removed is not None:
                    self.cache_bytes -= removed[1]
                return self._load_locked(
                    path=path, namespace=namespace, kind="frozen_model",
                    loader=loader, size_of=lambda _: None,
                )

    def close(self) -> None:
        with self._lock:
            self._entries.clear()
            self.cache_bytes = 0


def _retained_bytes(value: Any) -> int | None:
    """Account Python/numpy/tensor storage; unknown native objects aren't cached.

    This is a retention budget, not a claim about total process RSS or GPU peak:
    the call-local clone and feature working buffers remain separate allocations.
    """
    seen: set[int] = set()
    storages: set[tuple[str, int, int]] = set()

    def visit(item: Any) -> int:
        if id(item) in seen:
            return 0
        seen.add(id(item))
        size = sys.getsizeof(item)
        if hasattr(item, "untyped_storage") and hasattr(item, "device"):
            storage = item.untyped_storage()
            key = (str(item.device), storage.data_ptr(), storage.nbytes())
            if key not in storages:
                storages.add(key)
                size += storage.nbytes()
            return size
        if isinstance(item, dict):
            return size + sum(visit(key) + visit(child) for key, child in item.items())
        if isinstance(item, (tuple, list, set, frozenset)):
            return size + sum(visit(child) for child in item)
        if hasattr(item, "nbytes") and hasattr(item, "dtype"):
            # numpy owns/borrows storage; use a conservative full-buffer charge.
            return size + int(item.nbytes)
        if isinstance(item, (str, bytes, int, float, bool, type(None))):
            return size
        if isinstance(item, weakref.ReferenceType):
            return size  # A weak reference doesn't retain its target's storage.
        if type(item).__module__ == "torch" and type(item).__name__ in {"device", "dtype", "layout", "memory_format"}:
            return size
        if callable(item) and not hasattr(item, "__dict__"):
            return size
        attributes = getattr(item, "__dict__", None)
        if attributes is not None:
            if type(item).__module__.startswith("lightgbm"):
                raise TypeError("native booster retention cannot be measured here")
            return size + visit(attributes)
        raise TypeError("unknown inference resource storage")

    try:
        return visit(value)
    except (TypeError, AttributeError, RuntimeError, RecursionError):
        return None


class InferenceStageTimings:
    """Sequential checkpoints; no wall interval is called forward time."""

    def __init__(self) -> None:
        self._started = self._checkpoint = time.perf_counter()
        self.stages: dict[str, float] = {}

    def mark(self, name: str) -> None:
        now = time.perf_counter()
        self.stages[name] = self.stages.get(name, 0.0) + now - self._checkpoint
        self._checkpoint = now

    def result(self, **counters: Any) -> dict[str, Any]:
        if sys.platform == "linux":
            import resource

            counters["process_peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        torch = sys.modules.get("torch")
        cuda = getattr(torch, "cuda", None)
        initialized = getattr(cuda, "is_initialized", None)
        if callable(initialized) and initialized():
            counters["process_peak_gpu_allocated_bytes"] = cuda.max_memory_allocated()
        return {
            "schema_version": "advisory_inference_stage_timings_v1",
            "total_seconds": time.perf_counter() - self._started,
            "stage_seconds": dict(self.stages),
            "counters": counters,
        }
