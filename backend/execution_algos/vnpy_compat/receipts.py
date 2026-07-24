"""Deterministic K1-C compatibility receipt generation and authority readback."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import ExecutionAlgoPluginManifestV2
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CompatibilityStatusV1,
    VnpyCompatibilityFailureV1,
    VnpyCompatibilityReceiptV1,
)

from .locked_surface import PINNED_SOURCE_ROOT, extract_locked_surface_v1


_MAX_COMPATIBILITY_FAILURES = 256
_RETAINED_FAILURES_WHEN_TRUNCATED = _MAX_COMPATIBILITY_FAILURES - 1
_TRUNCATION_FIELD_PATH = "__failure_set__"
_TRUNCATION_REASON = "MINIQMT_VNPY_COMPAT_FAILURES_TRUNCATED"


class VnpyCompatibilityReceiptReadbackError(ValueError):
    def __init__(
        self,
        *,
        actual: VnpyCompatibilityReceiptV1,
        expected: VnpyCompatibilityReceiptV1,
    ) -> None:
        self.actual = actual
        self.expected = expected
        super().__init__(
            "vn.py compatibility receipt differs from pinned source authority: "
            f"actual={actual.receipt_sha256} expected={expected.receipt_sha256}"
        )


def _failure_identity(item: VnpyCompatibilityFailureV1) -> dict[str, str]:
    return {
        "field_path": item.field_path,
        "reason_code": item.reason_code,
        "context_sha256": item.context_sha256,
    }


def bound_compatibility_failures_v1(
    failures: tuple[VnpyCompatibilityFailureV1, ...],
) -> tuple[VnpyCompatibilityFailureV1, ...]:
    """Canonically retain all failures or append one explicit omitted-set marker."""

    if type(failures) is not tuple or any(type(item) is not VnpyCompatibilityFailureV1 for item in failures):
        raise TypeError("failures must be a tuple of VnpyCompatibilityFailureV1")
    ordered = tuple(sorted(failures, key=lambda item: item.sort_key_v1()))
    if len(ordered) <= _MAX_COMPATIBILITY_FAILURES:
        return ordered
    retained = ordered[:_RETAINED_FAILURES_WHEN_TRUNCATED]
    omitted = ordered[_RETAINED_FAILURES_WHEN_TRUNCATED:]
    omitted_identity = [_failure_identity(item) for item in omitted]
    marker = VnpyCompatibilityFailureV1.create(
        field_path=_TRUNCATION_FIELD_PATH,
        reason_code=_TRUNCATION_REASON,
        context={
            "omitted_count": len(omitted),
            "omitted_failure_set_sha256": hash_hex_v1(
                "miniqmt_vnpy_compatibility_omitted_failure_set_v1",
                omitted_identity,
            ),
        },
    )
    return (*retained, marker)


def build_vnpy_compatibility_receipt_v1(
    *,
    manifest: ExecutionAlgoPluginManifestV2,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> VnpyCompatibilityReceiptV1:
    """Generate PASSED or bounded FAILED from the real pinned-source evaluation."""

    if type(manifest) is not ExecutionAlgoPluginManifestV2 or not isinstance(source_root, Path):
        raise TypeError("manifest and source_root must use strict production types")
    surface = extract_locked_surface_v1(
        requirement=manifest.compatibility_requirement,
        source_root=source_root,
    )
    failures = bound_compatibility_failures_v1(surface.ordered_failures)
    return VnpyCompatibilityReceiptV1.create(
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        manifest_sha256=manifest.manifest_sha256,
        requirement_sha256=manifest.compatibility_requirement.requirement_sha256,
        surface_sha256=surface.surface_sha256,
        source_lock_sha256=surface.source_lock_sha256,
        method_signature_sha256=surface.method_signature_sha256,
        object_field_sha256=surface.object_field_sha256,
        characterization_sha256=surface.characterization_sha256,
        status=CompatibilityStatusV1.FAILED if failures else CompatibilityStatusV1.PASSED,
        ordered_failures=failures,
    )


def readback_vnpy_compatibility_receipt_v1(
    *,
    manifest: ExecutionAlgoPluginManifestV2,
    receipt: VnpyCompatibilityReceiptV1 | dict[str, Any],
    source_root: Path = PINNED_SOURCE_ROOT,
) -> VnpyCompatibilityReceiptV1:
    """Strictly read receipt and re-run the same pinned-source authority."""

    payload = receipt.model_dump(mode="python") if type(receipt) is VnpyCompatibilityReceiptV1 else receipt
    candidate = VnpyCompatibilityReceiptV1.model_validate(payload, strict=True)
    expected = build_vnpy_compatibility_receipt_v1(manifest=manifest, source_root=source_root)
    if candidate != expected:
        raise VnpyCompatibilityReceiptReadbackError(actual=candidate, expected=expected)
    return candidate


def build_current_three_compatibility_receipts_v1(
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyCompatibilityReceiptV1, ...]:
    """Generate receipts for all current code-owned manifests without defaults."""

    if not isinstance(source_root, Path):
        raise TypeError("source_root must be pathlib.Path")
    from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v2

    return tuple(
        build_vnpy_compatibility_receipt_v1(manifest=manifest, source_root=source_root)
        for manifest in current_three_manifests_v2()
    )


__all__ = [
    "VnpyCompatibilityReceiptReadbackError",
    "bound_compatibility_failures_v1",
    "build_current_three_compatibility_receipts_v1",
    "build_vnpy_compatibility_receipt_v1",
    "readback_vnpy_compatibility_receipt_v1",
]
