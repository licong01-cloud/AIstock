"""K5 remains a shadow-only composition with no product-root import."""

from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[3]
_PRODUCT_ROOTS = (
    "backend/execution_algos/__init__.py",
    "backend/execution_algos/vnpy_compat/__init__.py",
    "backend/services/miniqmt_execution_runtime/__init__.py",
    "backend/services/miniqmt_execution_runtime/client.py",
    "backend/services/miniqmt_execution_runtime/kernel_creation.py",
    "backend/services/miniqmt_execution_runtime/kernel_delivery.py",
)


def test_k5_shadow_catalog_is_not_imported_by_known_product_roots() -> None:
    for relative_path in _PRODUCT_ROOTS:
        source = (_REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert "k5_shadow_catalog" not in source
        assert "k5_plugin_manifests" not in source
        assert "k5_plugin_factories" not in source


def test_k5_does_not_add_algo_code_branches_to_kernel_or_client() -> None:
    service_root = _REPO_ROOT / "backend/services/miniqmt_execution_runtime"
    targets = tuple(sorted(service_root.glob("kernel*.py"))) + (service_root / "client.py",)

    for path in targets:
        source = path.read_text(encoding="utf-8")
        assert '"ICEBERG"' not in source
        assert '"STOP"' not in source
