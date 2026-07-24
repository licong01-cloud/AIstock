"""Shadow-only vn.py compatibility validation exports."""

from .import_boundary import (
    ImportBoundaryFailureV1,
    ImportBoundaryTargetV1,
    PluginImportBoundaryError,
    PluginImportBoundaryReceiptV1,
    validate_plugin_import_boundaries_v1,
)
from .locked_surface import (
    PINNED_SOURCE_ROOT,
    LockedSurfaceV1,
    PinnedSourceManifestV1,
    extract_locked_surface_v1,
    load_pinned_source_manifest_v1,
    validate_pinned_source_v1,
)
from .receipts import (
    VnpyCompatibilityReceiptReadbackError,
    build_current_three_compatibility_receipts_v1,
    build_vnpy_compatibility_receipt_v1,
    readback_vnpy_compatibility_receipt_v1,
)

__all__ = [
    "ImportBoundaryFailureV1",
    "ImportBoundaryTargetV1",
    "PluginImportBoundaryError",
    "PluginImportBoundaryReceiptV1",
    "validate_plugin_import_boundaries_v1",
    "PINNED_SOURCE_ROOT",
    "LockedSurfaceV1",
    "PinnedSourceManifestV1",
    "extract_locked_surface_v1",
    "load_pinned_source_manifest_v1",
    "validate_pinned_source_v1",
    "VnpyCompatibilityReceiptReadbackError",
    "build_current_three_compatibility_receipts_v1",
    "build_vnpy_compatibility_receipt_v1",
    "readback_vnpy_compatibility_receipt_v1",
]
