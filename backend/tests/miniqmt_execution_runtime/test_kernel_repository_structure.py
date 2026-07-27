from __future__ import annotations

import hashlib
import importlib
import inspect
import json
from pathlib import Path
import subprocess
import sys

from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    KernelRepositorySchemaError,
    PostgresMiniQMTKernelRepository,
)


_PUBLIC_SIGNATURE_SHA256 = "80dd1fa076bf94638adbc0bd51fb8d740a498cdc01d5b0780c0912319ae2d8a3"
_PRIVATE_MODULES = (
    "backend.services.miniqmt_execution_runtime.kernel_repository_common",
    "backend.services.miniqmt_execution_runtime.kernel_repository_projection",
    "backend.services.miniqmt_execution_runtime.kernel_repository_schema",
    "backend.services.miniqmt_execution_runtime.kernel_repository_event_delivery",
    "backend.services.miniqmt_execution_runtime.kernel_repository_k2b",
    "backend.services.miniqmt_execution_runtime.kernel_repository_transition_outbox",
    "backend.services.miniqmt_execution_runtime.kernel_repository_timer_session",
)
_MIGRATION_SHA256 = {
    "backend/migrations/miniqmt_execution_kernel_k2_20260725.preflight.sql": (
        "e2a244d0090aa4b2ead240838261a2da79d87a4a3f050cdd69efd77fb5187ede"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2_20260725.sql": (
        "24b4e1894f93f1383d7690ff145c55e100a26cecfc9e60a9070b71a57524d083"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2_20260725.rollback.sql": (
        "cb408aafd8bc9032594a129ee259807f8a7480a92c34332f347cf93e8576c749"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2c_timer_reclaim_20260727.preflight.sql": (
        "833e7af7f3a12b1cbd8db29f768088752b4bf4fcc7aafd4afa91774464698d21"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2c_timer_reclaim_20260727.sql": (
        "3552277b61c4035924bb787396565101a1403774a0c2c72ba5d8356965d3ec50"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2c_timer_reclaim_20260727.rollback.sql": (
        "11ca28e7981a4898fdcecc14067852f4e1129bef0a1a8bb31298ac886312fd13"
    ),
}


def _normalized_signature_sha256() -> str:
    empty = inspect.Signature.empty

    def normalize(value: object) -> str:
        if value is empty:
            return "<empty>"
        if callable(value):
            return f"{value.__module__}.{value.__qualname__}"  # type: ignore[attr-defined]
        return repr(value)

    names = ["__init__"] + sorted(
        name
        for name in dir(PostgresMiniQMTKernelRepository)
        if not name.startswith("_") and callable(getattr(PostgresMiniQMTKernelRepository, name))
    )
    payload = {}
    for name in names:
        signature = inspect.signature(getattr(PostgresMiniQMTKernelRepository, name))
        payload[name] = {
            "parameters": [
                (parameter.name, parameter.kind.name, normalize(parameter.annotation), normalize(parameter.default))
                for parameter in signature.parameters.values()
            ],
            "return": normalize(signature.return_annotation),
        }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def test_repository_public_api_and_exception_identity_are_characterized() -> None:
    assert _normalized_signature_sha256() == _PUBLIC_SIGNATURE_SHA256
    assert KernelRepositoryConflict.__name__ == "KernelRepositoryConflict"
    assert KernelRepositorySchemaError.__name__ == "KernelRepositorySchemaError"
    assert KernelRepositoryCommitUnknown.__name__ == "KernelRepositoryCommitUnknown"


def test_repository_private_responsibility_modules_have_one_public_facade() -> None:
    modules = [importlib.import_module(name) for name in _PRIVATE_MODULES]

    assert [base.__name__ for base in PostgresMiniQMTKernelRepository.__mro__] == [
        "PostgresMiniQMTKernelRepository",
        "KernelRepositorySchemaMixin",
        "KernelRepositoryEventDeliveryMixin",
        "KernelRepositoryK2BMixin",
        "KernelRepositoryTransitionOutboxMixin",
        "KernelRepositoryTimerSessionMixin",
        "KernelRepositoryBase",
        "object",
    ]
    assert all(not hasattr(module, "PostgresMiniQMTKernelRepository") for module in modules)

    method_owners: dict[str, list[str]] = {}
    for owner in PostgresMiniQMTKernelRepository.__mro__:
        if owner is object:
            continue
        for name, value in owner.__dict__.items():
            if callable(value) and not (name.startswith("__") and name.endswith("__")):
                method_owners.setdefault(name, []).append(owner.__name__)
    assert method_owners
    assert all(len(owners) == 1 for owners in method_owners.values())


def test_repository_fresh_process_import_has_no_product_runtime_side_effect() -> None:
    script = """
import json
import sys
from backend.services.miniqmt_execution_runtime.kernel_repository import PostgresMiniQMTKernelRepository
forbidden_names = {
    'backend.services.miniqmt_execution_runtime.runtime',
    'backend.services.miniqmt_execution_runtime.gateway',
    'backend.services.miniqmt_execution_runtime.client',
}
forbidden = sorted(forbidden_names.intersection(sys.modules))
print(json.dumps({'class': PostgresMiniQMTKernelRepository.__name__, 'forbidden': forbidden}, sort_keys=True))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    assert payload == {"class": "PostgresMiniQMTKernelRepository", "forbidden": []}


def test_repository_refactor_keeps_k2_migration_chain_byte_identical() -> None:
    for path, expected_sha256 in _MIGRATION_SHA256.items():
        raw = Path(path).read_bytes()
        assert b"\r" not in raw.replace(b"\r\n", b"")
        canonical_lf = raw.replace(b"\r\n", b"\n")
        assert hashlib.sha256(canonical_lf).hexdigest() == expected_sha256
