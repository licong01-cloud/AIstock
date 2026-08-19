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

# Modules fixed by BUG-1134 with ``UTC = timezone.utc``. Only
# ``control_store`` is in the import-time closure of the WSL entry script
# (and is the module named in the production traceback). The other three are
# adjacent MiniQMT execution-path modules whose identical ``from datetime
# import UTC`` was found by the closure audit; they load lazily (not during
# the WSL inference run) and were hardened with the same zero-drift alias.
# They are guarded at file level below, independent of closure membership.
FIXED_MODULES = (
    "backend.services.dataset_release.control_store",
    "backend.infra.qmt_client",
    "backend.infra.realtime_quote_subscriber",
    "backend.execution_algos.adaptive_is.contracts",
)

# Chain modules that must be reachable in the import-time closure:
# scripts/strategy_package_live_inference.py -> advisory_input_projection ->
# canonical_pit_compatibility -> qe_dataset_contract ->
# canonical_pit_dataset_consumer -> dataset_release.cas_store -> control_store.
CHAIN_MODULES = (
    "backend.services.strategy_package.advisory_input_projection",
    "backend.services.strategy_package.canonical_pit_compatibility",
    "backend.services.quantevolver.qe_dataset_contract",
    "backend.services.canonical_pit_dataset_consumer",
    "backend.services.dataset_release.cas_store",
    "backend.services.dataset_release.control_store",
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


def _parse_module(path: Path) -> ast.Module:
    # utf-8-sig strips a leading BOM, which the real importer (PEP 263)
    # accepts but ast.parse rejects.
    return ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))


def _module_path(module: str) -> Path:
    return ROOT / (module.replace(".", "/") + ".py")


def _expand_with_package_inits(paths: list[Path]) -> list[Path]:
    """Add every ancestor package ``__init__.py`` of each resolved module.

    Importing ``a.b.c`` executes the ``__init__.py`` of ``a`` and ``a.b``
    first; those files can themselves raise ImportError under Python 3.10
    and their own imports pull in further modules, so they must be closure
    members for the scan to follow them.
    """

    expanded: list[Path] = []
    for path in paths:
        expanded.append(path)
        directory = path.parent
        while directory != ROOT and ROOT in directory.parents:
            package_init = directory / "__init__.py"
            if package_init.is_file():
                expanded.append(package_init)
            directory = directory.parent
    return expanded


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


def _iter_closure_imports(tree: ast.Module) -> list[ast.Import | ast.ImportFrom]:
    """Import statements reachable when the module is imported or used.

    Follows module-level control flow (try/except, if/else, loops) but skips
    ``if TYPE_CHECKING:`` branches (never executed at runtime). Imports
    nested in function/class bodies are lazy and normally irrelevant to
    import-time failures like BUG-1134 — EXCEPT when the module defines a
    PEP 562 module-level ``__getattr__``: such a module has explicitly opted
    into lazy attribute loading, and any attribute access executes those
    function bodies (e.g. ``backend/execution_algos/__init__.py`` loading its
    registry), so their imports are closure-reachable too.
    """

    def _is_type_checking_test(test: ast.expr) -> bool:
        if isinstance(test, ast.Name):
            return test.id == "TYPE_CHECKING"
        return (
            isinstance(test, ast.Attribute)
            and test.attr == "TYPE_CHECKING"
            and isinstance(test.value, ast.Name)
            and test.value.id == "typing"
        )

    found: list[ast.Import | ast.ImportFrom] = []
    has_module_getattr = any(
        isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef))
        and statement.name == "__getattr__"
        for statement in tree.body
    )

    def visit_statements(statements: list[ast.stmt]) -> None:
        for statement in statements:
            if isinstance(statement, (ast.Import, ast.ImportFrom)):
                found.append(statement)
            elif isinstance(statement, ast.If):
                if _is_type_checking_test(statement.test):
                    visit_statements(statement.orelse)
                else:
                    visit_statements(statement.body)
                    visit_statements(statement.orelse)
            elif isinstance(statement, (ast.Try, ast.While, ast.For)):
                visit_statements(statement.body)
                visit_statements(statement.orelse)
                if isinstance(statement, ast.Try):
                    for handler in statement.handlers:
                        visit_statements(handler.body)
                    visit_statements(statement.finalbody)
            elif isinstance(statement, (ast.With, ast.AsyncWith)):
                visit_statements(statement.body)
            elif has_module_getattr and isinstance(
                statement, (ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                for node in ast.walk(statement):
                    if isinstance(node, (ast.Import, ast.ImportFrom)):
                        found.append(node)

    visit_statements(tree.body)
    return found


def _resolve_imports(path: Path) -> list[Path]:
    """Resolve the import-time (top-level) local imports of one module."""

    tree = _parse_module(path)
    resolved: list[Path] = []
    for node in _iter_closure_imports(tree):
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
    return _expand_with_package_inits(resolved)


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
    tree = _parse_module(path)
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
    # Guard against a silently vacuous closure: the computation must reach
    # every module on the known production failure chain, otherwise the scan
    # below protects nothing.
    expected = {_module_path(module) for module in CHAIN_MODULES}
    missing = sorted(str(path.relative_to(ROOT)) for path in expected - closure)
    assert not missing, f"closure does not reach known chain modules: {missing}"
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
