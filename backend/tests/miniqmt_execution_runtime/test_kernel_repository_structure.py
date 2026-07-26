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


_PUBLIC_SIGNATURE_SHA256 = "7045f3c27dc6c213ca52cf949de8585ccaa90e0a4276f3a4679f6202aaee3746"
_PRIVATE_MODULES = (
    "backend.services.miniqmt_execution_runtime.kernel_repository_common",
    "backend.services.miniqmt_execution_runtime.kernel_repository_projection",
    "backend.services.miniqmt_execution_runtime.kernel_repository_schema",
    "backend.services.miniqmt_execution_runtime.kernel_repository_event_delivery",
    "backend.services.miniqmt_execution_runtime.kernel_repository_transition_outbox",
    "backend.services.miniqmt_execution_runtime.kernel_repository_timer_session",
)
_MIGRATION_SHA256 = {
    "backend/migrations/miniqmt_execution_kernel_k2_20260725.preflight.sql": (
        "785d438d6d8b388f7951fea394f41344f3dd98bd9453849ddc76a04fd2c4852c"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2_20260725.sql": (
        "f6331a8a8e1118b8fe291c4f63c2ff8a15a359cb67104b6b4ce895a56376de8c"
    ),
    "backend/migrations/miniqmt_execution_kernel_k2_20260725.rollback.sql": (
        "7ab203531741f4f1d140c9f76a3c98ae411a02961b302c0897e538c5d9c2cc71"
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


def test_repository_refactor_keeps_k2_migration_triplet_byte_identical() -> None:
    for path, expected_sha256 in _MIGRATION_SHA256.items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == expected_sha256
