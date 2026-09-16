"""Compact Python 3.10 compatibility contract for the WSL inference chain."""

from __future__ import annotations

import ast
import hashlib
import importlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
FIXED_MODULES = (
    "backend.services.dataset_release.control_store",
    "backend.infra.qmt_client",
    "backend.infra.realtime_quote_subscriber",
    "backend.execution_algos.adaptive_is.contracts",
)
CHAIN_MODULES = (
    "backend.services.strategy_package.advisory_input_projection",
    "backend.services.strategy_package.canonical_pit_compatibility",
    "backend.services.quantevolver.qe_dataset_contract",
    "backend.services.canonical_pit_dataset_consumer",
    "backend.services.dataset_release.cas_store",
    "backend.services.dataset_release.control_store",
)
FORBIDDEN_IMPORTS = {
    ("datetime", "UTC"),
    ("enum", "StrEnum"),
    ("typing", "Self"),
    ("typing", "TypeAliasType"),
}
FORBIDDEN_MODULES = {"tomllib"}
FORBIDDEN_NAMES = {"ExceptionGroup", "BaseExceptionGroup"}


def _module_path(module: str) -> Path:
    return ROOT / f"{module.replace('.', '/')}.py"


def _violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            found.extend(
                f"{node.lineno}:from {node.module} import {alias.name}"
                for alias in node.names
                if (node.module, alias.name) in FORBIDDEN_IMPORTS
            )
            if node.module in FORBIDDEN_MODULES:
                found.append(f"{node.lineno}:from {node.module} import")
        elif isinstance(node, ast.Import):
            found.extend(
                f"{node.lineno}:import {alias.name}"
                for alias in node.names
                if alias.name.split(".")[0] in FORBIDDEN_MODULES
            )
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            found.append(f"{node.lineno}:{node.id}")
        elif (
            isinstance(node, ast.Attribute)
            and node.attr == "UTC"
            and isinstance(node.value, ast.Name)
            and node.value.id == "datetime"
        ):
            found.append(f"{node.lineno}:datetime.UTC")
    return found


def test_wsl_inference_contract_modules_have_no_python311_only_symbols() -> None:
    modules = tuple(dict.fromkeys((*FIXED_MODULES, *CHAIN_MODULES)))
    failures = {module: _violations(_module_path(module)) for module in modules}
    assert not {module: found for module, found in failures.items() if found}


@pytest.mark.parametrize("module_name", FIXED_MODULES)
def test_utc_alias_is_timezone_utc(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert module.UTC is timezone.utc
    assert datetime.now(module.UTC).utcoffset().total_seconds() == 0


def test_wsl_inference_chain_imports_without_side_effectful_construction() -> None:
    for module_name in (*FIXED_MODULES, *CHAIN_MODULES):
        importlib.import_module(module_name)


def test_control_store_datetime_and_cas_golden_vectors() -> None:
    from zoneinfo import ZoneInfo

    from backend.services.dataset_release import control_store
    from backend.services.dataset_release.cas_store import canonical_json_bytes

    value = datetime(2026, 8, 19, 9, 30, 0, 123456, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert control_store._iso_datetime(value) == "2026-08-19T01:30:00.123456+00:00"
    with pytest.raises(control_store.StateConflict):
        control_store._aware_utc(datetime(2026, 8, 19), field="asof")

    blob = canonical_json_bytes({"b": 1, "a": "x"})
    assert blob == b'{"a":"x","b":1}\n'
    assert hashlib.sha256(blob).hexdigest() == ("b9726bbcdf05823038cfdf7612b50329709a519a5da6f1ac21671f6b5dd31dc2")
