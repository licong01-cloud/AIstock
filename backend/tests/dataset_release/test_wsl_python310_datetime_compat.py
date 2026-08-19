"""BUG-1134: WSL live inference must import under Python 3.10.

Production failure (2026-08-19, runs ``simrun_14e5719768c8b6b9`` MiniQMT and
``simrun_d1ca5bba018b72b5`` LocalSIM, reason_code
``strategy_package_wsl_inference_failed``, stage ``pre_run``,
``broker_called=false``): ``scripts/strategy_package_live_inference.py``
failed in WSL ``rdagent-gpu`` (Python 3.10) with
``ImportError: cannot import name 'UTC' from 'datetime'`` raised by
``backend/services/dataset_release/control_store.py``. ``datetime.UTC`` exists
only in Python 3.11+; it is the identical singleton ``datetime.timezone.utc``
(same tzinfo object, zero offset, identical isoformat output), so the fix is a
pure compatibility alias with zero semantic, serialization, or CAS/hash drift.

These tests pin:

* the transitive import closure of the WSL live-inference entry script stays
  free of Python 3.11+-only symbols (``datetime.UTC``, ``enum.StrEnum``,
  ``tomllib``, ``ExceptionGroup``, ``typing.Self``, ``typing.TypeAliasType``);
* every fixed module's ``UTC`` *is* ``timezone.utc`` (identity, not just
  equality) and produces timezone-aware datetimes with zero offset;
* timezone-aware canonicalization and CAS identity are byte-for-byte
  unchanged (golden vectors);
* the full Strategy Package live-inference import chain imports cleanly.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]

# Modules whose ``from datetime import UTC`` broke the WSL Python 3.10 import
# closure and were fixed by BUG-1134 with ``UTC = timezone.utc``.
FIXED_MODULES = (
    "backend.services.dataset_release.control_store",
    "backend.infra.qmt_client",
    "backend.infra.realtime_quote_subscriber",
    "backend.execution_algos.adaptive_is.contracts",
)

# Remaining links of the production failure import chain:
# scripts/strategy_package_live_inference.py -> advisory_input_projection ->
# canonical_pit_compatibility -> qe_dataset_contract ->
# canonical_pit_dataset_consumer -> dataset_release.cas_store -> control_store.
CHAIN_MODULES = (
    "backend.services.strategy_package.advisory_input_projection",
    "backend.services.strategy_package.canonical_pit_compatibility",
    "backend.services.quantevolver.qe_dataset_contract",
    "backend.services.canonical_pit_dataset_consumer",
    "backend.services.dataset_release.cas_store",
)

# Symbols that only exist on Python 3.11+ and therefore must never appear in
# the WSL live-inference closure (rdagent-gpu runs Python 3.10).
_FORBIDDEN_IMPORT_FROM = {
    ("datetime", "UTC"),
    ("enum", "StrEnum"),
    ("typing", "Self"),
    ("typing", "TypeAliasType"),
}
_FORBIDDEN_MODULES = {"tomllib"}
_FORBIDDEN_NAMES = {"ExceptionGroup", "BaseExceptionGroup"}


def _module_path(module: str) -> Path:
    return ROOT / (module.replace(".", "/") + ".py")


def _add_module_candidates(base: Path, names: list[str], resolved: list[Path]) -> None:
    """Resolve a module path plus any importable submodule names."""

    candidates = [base] + [base.joinpath(*name.split(".")) for name in names]
    for candidate_base in candidates:
        module_file = candidate_base.with_suffix(".py")
        if module_file.is_file():
            resolved.append(module_file)
            continue
        package_init = candidate_base / "__init__.py"
        if package_init.is_file():
            resolved.append(package_init)


def _resolve_imports(path: Path) -> list[Path]:
    """Resolve the local imports of one module to file paths."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    resolved: list[Path] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                for end in range(len(parts), 0, -1):
                    prefix = ROOT.joinpath(*parts[:end])
                    if prefix.with_suffix(".py").is_file():
                        resolved.append(prefix.with_suffix(".py"))
                        break
                    if (prefix / "__init__.py").is_file():
                        resolved.append(prefix / "__init__.py")
                        break
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = path.parent
                for _ in range(node.level - 1):
                    base = base.parent
                if node.module:
                    base = base.joinpath(*node.module.split("."))
                _add_module_candidates(base, [alias.name for alias in node.names], resolved)
            elif node.module:
                base = ROOT.joinpath(*node.module.split("."))
                _add_module_candidates(base, [alias.name for alias in node.names], resolved)
    return resolved


def _live_inference_closure() -> set[Path]:
    """Transitive local-import closure of the WSL live-inference entry script."""

    entry = ROOT / "scripts" / "strategy_package_live_inference.py"
    closure: set[Path] = set()
    stack = [entry]
    while stack:
        current = stack.pop()
        if current in closure or not current.is_file():
            continue
        try:
            current.relative_to(ROOT)
        except ValueError:
            continue  # third-party / stdlib resolved outside the repo
        if "site-packages" in current.parts or "xtquant" in current.parts:
            continue  # vendored/third-party code is outside the fix boundary
        closure.add(current)
        stack.extend(_resolve_imports(current))
    return closure


def _py311_only_violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if (node.module, alias.name) in _FORBIDDEN_IMPORT_FROM:
                    violations.append(
                        f"line {node.lineno}: from {node.module} import {alias.name}"
                    )
            if node.module in _FORBIDDEN_MODULES:
                violations.append(f"line {node.lineno}: from {node.module} import ...")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in _FORBIDDEN_MODULES:
                    violations.append(f"line {node.lineno}: import {alias.name}")
        elif isinstance(node, ast.Attribute):
            if (
                node.attr == "UTC"
                and isinstance(node.value, ast.Name)
                and node.value.id == "datetime"
            ):
                violations.append(f"line {node.lineno}: datetime.UTC attribute access")
        elif isinstance(node, ast.Name) and node.id in _FORBIDDEN_NAMES:
            violations.append(f"line {node.lineno}: {node.id}")
    return violations


def test_fixed_modules_have_no_python311_only_symbols() -> None:
    failures = {
        module: _py311_only_violations(_module_path(module))
        for module in FIXED_MODULES
    }
    failures = {module: found for module, found in failures.items() if found}
    assert not failures, f"Python 3.11+-only symbols remain: {failures}"


def test_live_inference_closure_has_no_python311_only_symbols() -> None:
    closure = _live_inference_closure()
    assert closure, "closure computation returned no files"
    failures: dict[str, list[str]] = {}
    for path in sorted(closure):
        found = _py311_only_violations(path)
        if found:
            failures[str(path.relative_to(ROOT))] = found
    assert not failures, (
        "WSL live-inference closure uses Python 3.11+-only symbols "
        f"(rdagent-gpu runs Python 3.10): {failures}"
    )


@pytest.mark.parametrize("module_name", FIXED_MODULES)
def test_utc_is_timezone_utc_singleton(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert module.UTC is timezone.utc
    now = datetime.now(module.UTC)
    assert now.tzinfo is timezone.utc
    assert now.utcoffset() == timezone.utc.utcoffset(None)
    assert now.utcoffset().total_seconds() == 0
    assert now.isoformat().endswith("+00:00")


def test_live_inference_chain_imports() -> None:
    # Importing must be side-effect free with respect to broker, orders, and
    # databases: these are pure module imports, no clients are constructed.
    for module_name in (*FIXED_MODULES, *CHAIN_MODULES):
        importlib.import_module(module_name)


def test_control_store_utc_now_format_golden() -> None:
    from backend.services.dataset_release import control_store

    stamp = control_store.utc_now()
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00", stamp
    ), stamp


def test_control_store_iso_datetime_golden_vectors() -> None:
    from zoneinfo import ZoneInfo

    from backend.services.dataset_release import control_store

    utc_value = datetime(2026, 8, 19, 1, 2, 3, 456789, tzinfo=control_store.UTC)
    assert (
        control_store._iso_datetime(utc_value) == "2026-08-19T01:02:03.456789+00:00"
    )
    shanghai_value = datetime(
        2026, 8, 19, 9, 30, 0, 123456, tzinfo=ZoneInfo("Asia/Shanghai")
    )
    assert (
        control_store._iso_datetime(shanghai_value)
        == "2026-08-19T01:30:00.123456+00:00"
    )
    assert control_store._iso_datetime(None) is None


def test_control_store_aware_utc_semantics_unchanged() -> None:
    from zoneinfo import ZoneInfo

    from backend.services.dataset_release import control_store

    naive = datetime(2026, 8, 19, 1, 2, 3)
    with pytest.raises(control_store.StateConflict):
        control_store._aware_utc(naive, field="asof")
    shanghai_value = datetime(2026, 8, 19, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    converted = control_store._aware_utc(shanghai_value, field="asof")
    assert converted.tzinfo is timezone.utc
    assert converted == datetime(2026, 8, 19, 1, 30, tzinfo=timezone.utc)


def test_cas_canonical_json_golden_vectors() -> None:
    # CAS identity is byte-identical before and after the alias change:
    # ``timezone.utc`` and ``datetime.UTC`` serialize identically.
    from backend.services.dataset_release.cas_store import canonical_json_bytes

    payload = {"b": 1, "a": "x"}
    blob = canonical_json_bytes(payload)
    assert blob == b'{"a":"x","b":1}\n'
    assert (
        hashlib.sha256(blob).hexdigest()
        == "b9726bbcdf05823038cfdf7612b50329709a519a5da6f1ac21671f6b5dd31dc2"
    )
    ts_payload = {"ts": "2026-08-19T01:30:00.123456+00:00"}
    ts_blob = canonical_json_bytes(ts_payload)
    assert ts_blob == b'{"ts":"2026-08-19T01:30:00.123456+00:00"}\n'
    assert (
        hashlib.sha256(ts_blob).hexdigest()
        == "424838e31287961507a70155efb2d4deffd153aa58ed5f96385fb4105ccdc46a"
    )
