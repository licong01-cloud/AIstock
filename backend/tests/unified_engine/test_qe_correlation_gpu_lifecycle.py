from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace

import numpy as np

from backend.services.quantevolver import correlation_engine as ce


def _reset_gpu_backend(monkeypatch) -> None:
    monkeypatch.setattr(ce, "_gpu_backend", ce._GPU_BACKEND_UNRESOLVED)


def test_correlation_engine_import_does_not_import_torch_in_fresh_process() -> None:
    script = r"""
import importlib.abc
import sys

class RejectTorch(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "torch" or fullname.startswith("torch."):
            raise AssertionError(f"unexpected eager torch import: {fullname}")
        return None

sys.meta_path.insert(0, RejectTorch())
import backend.services.quantevolver.correlation_engine
assert "torch" not in sys.modules
print("correlation_engine_import_without_torch=passed")
"""
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "correlation_engine_import_without_torch=passed" in completed.stdout


def test_gpu_backend_is_resolved_only_on_first_compute_request(monkeypatch) -> None:
    _reset_gpu_backend(monkeypatch)
    monkeypatch.setattr(ce, "is_wsl_runtime", lambda: True)
    calls: list[str] = []

    class FakeMatrix:
        def __matmul__(self, other):
            calls.append("gemm")
            return self

    class FakeCuda:
        @staticmethod
        def is_available() -> bool:
            calls.append("is_available")
            return True

        @staticmethod
        def synchronize() -> None:
            calls.append("synchronize")

        @staticmethod
        def get_device_name(_index: int) -> str:
            return "fake-gpu"

    fake_torch = SimpleNamespace(
        cuda=FakeCuda(),
        version=SimpleNamespace(cuda="fake-cuda"),
        randn=lambda *_args, **_kwargs: FakeMatrix(),
    )

    def fake_import(name: str):
        calls.append(f"import:{name}")
        assert name == "torch"
        return fake_torch

    monkeypatch.setattr(ce.importlib, "import_module", fake_import)

    assert calls == []
    assert ce._resolve_gpu_backend() is fake_torch
    assert calls == ["import:torch", "is_available", "gemm", "synchronize"]
    assert ce._resolve_gpu_backend() is fake_torch
    assert calls == ["import:torch", "is_available", "gemm", "synchronize"]


def test_unavailable_cuda_is_cached_and_uses_cpu_blas(monkeypatch) -> None:
    _reset_gpu_backend(monkeypatch)
    monkeypatch.setattr(ce, "is_wsl_runtime", lambda: True)
    imports: list[str] = []
    fake_torch = SimpleNamespace(
        cuda=SimpleNamespace(is_available=lambda: False),
    )

    def fake_import(name: str):
        imports.append(name)
        return fake_torch

    monkeypatch.setattr(ce.importlib, "import_module", fake_import)
    engine = ce.CorrelationEngine(loader=object())
    section = np.array(
        [
            [float(i), float(i * 2), float(100 - i)]
            for i in range(40)
        ],
        dtype=float,
    )

    result = engine._cross_sectional_spearman_gemm(
        ce.pd.DataFrame(section, columns=["a", "b", "c"])
    )

    assert imports == ["torch"]
    assert result is not None
    assert result[0, 1] > 0.99
    assert result[0, 2] < -0.99
    assert ce._resolve_gpu_backend() is None
    assert imports == ["torch"]


def test_non_wsl_backend_never_imports_torch_for_correlation(monkeypatch) -> None:
    _reset_gpu_backend(monkeypatch)
    monkeypatch.setattr(ce, "is_wsl_runtime", lambda: False)
    imports: list[str] = []
    monkeypatch.setattr(
        ce.importlib,
        "import_module",
        lambda name: imports.append(name),
    )

    assert ce._resolve_gpu_backend() is None
    assert ce._resolve_gpu_backend() is None
    assert imports == []
