"""Local capacity and dependency checks for read-only, concurrent historical replay."""
from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
import shutil
import sys

from .errors import AdvisoryModelFirstError

REPLAY_THREADS = 2
MINIMUM_FREE_MEMORY_BYTES = 512 * 1024**2
MINIMUM_FREE_DISK_BYTES = 128 * 1024**2


def _local_capacity(output_root):
    import psutil

    path = Path(output_root).resolve()
    while not path.exists() and path != path.parent:
        path = path.parent
    return {
        "available_memory_bytes": int(psutil.virtual_memory().available),
        "free_disk_bytes": int(shutil.disk_usage(path).free),
        "process_rss_bytes": int(psutil.Process().memory_info().rss),
        "logical_cpus": int(psutil.cpu_count() or 1),
    }


def probe_replay_capacity(output_root):
    # CPU busy is not an exclusivity signal: two bounded threads can share CPU.
    # These are working-space budgets, not statistical/model advancement gates.
    try:
        snapshot = _local_capacity(output_root)
    except (ImportError, OSError, RuntimeError) as exc:
        return {"status": "WAITING_RESOURCE", "reason_code": "LOCAL_CAPACITY_UNAVAILABLE",
                "error_type": type(exc).__name__}
    available = (snapshot["available_memory_bytes"] >= MINIMUM_FREE_MEMORY_BYTES
                 and snapshot["free_disk_bytes"] >= MINIMUM_FREE_DISK_BYTES)
    return {"schema_version": "advisory_entry_replay_capacity_v1", **snapshot,
            "status": "REPLAY_CAPACITY_AVAILABLE" if available else "WAITING_RESOURCE",
            "reason_code": None if available else "LOCAL_CAPACITY_EXHAUSTED",
            "maximum_threads": REPLAY_THREADS, "qe_idle_required": False}


def validate_replay_dependencies():
    try:
        library = import_module("lightgbm")
        import_module("threadpoolctl")
    except (ImportError, OSError) as exc:
        raise AdvisoryModelFirstError(
            "Replay predictor unavailable; run with the explicitly selected AIstock Python environment; no automatic installation",
            reason_code="ADVISORY_ENTRY_REPLAY_DEPENDENCY_UNAVAILABLE",
        ) from exc
    return {"python_executable": sys.executable, "lightgbm_version": library.__version__,
            "maximum_threads": REPLAY_THREADS, "dependency_installed": False}


def load_replay_booster(path):
    validate_replay_dependencies()
    library = import_module("lightgbm")
    # LightGBM ignores constructor params when restoring a saved model. Bound
    # each inference call instead; do not mutate/reset the frozen booster.
    return _ReplayBooster(library.Booster(model_file=str(path)))


class _ReplayBooster:
    def __init__(self, booster):
        self._booster = booster

    def feature_name(self):
        return self._booster.feature_name()

    def predict(self, data, **kwargs):
        kwargs["num_threads"] = REPLAY_THREADS
        return self._booster.predict(data, **kwargs)


@contextmanager
def replay_thread_budget():
    """Only the isolated replay process's native BLAS/OpenMP pools are limited."""
    from threadpoolctl import threadpool_limits

    with threadpool_limits(limits=REPLAY_THREADS):
        yield
