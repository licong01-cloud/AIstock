"""Static and isolated-process import boundary for shadow-only K1 modules."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal


_FORBIDDEN_IMPORT_PREFIXES = (
    "aiohttp",
    "asyncpg",
    "backend.agents",
    "backend.data_access",
    "backend.data_service",
    "backend.db",
    "backend.infra",
    "backend.models",
    "backend.quant_models",
    "backend.repositories",
    "backend.routers",
    "backend.schedulers",
    "backend.services.miniqmt_execution_runtime.client",
    "backend.services.miniqmt_execution_runtime.gateway",
    "backend.services.miniqmt_execution_runtime.oms",
    "backend.services.miniqmt_execution_runtime.repository",
    "backend.services.miniqmt_execution_runtime.runtime",
    "backend.services.paper_trading",
    "backend.services.quantevolver",
    "backend.services.selection",
    "backend.services.simulation_runtime",
    "backend.services.strategy_package",
    "backend.strategies",
    "fastapi",
    "ftplib",
    "http.client",
    "httpx",
    "psycopg",
    "psycopg2",
    "redis",
    "requests",
    "smtplib",
    "socket",
    "sqlite3",
    "sqlalchemy",
    "urllib",
    "vnpy",
    "websockets",
    "xmlrpc",
    "xtquant",
)
_ALLOWED_INTERNAL_IMPORT_PREFIXES = (
    "backend.execution_algos.vnpy_compat",
    "backend.execution_algos.vnpy_style.plugin_manifests",
    "backend.services.miniqmt_execution_runtime.deterministic_context",
    "backend.services.miniqmt_execution_runtime.plugin_canonical",
    "backend.services.miniqmt_execution_runtime.plugin_contracts",
    "backend.services.miniqmt_execution_runtime.plugin_registry",
)
_FORBIDDEN_OWNER_SYMBOLS = frozenset({"BaseGateway", "EventEngine", "MainEngine", "OmsEngine"})
_FORBIDDEN_OWNER_CLASS_SUFFIXES = ("BaseGateway", "EventEngine", "Gateway", "MainEngine", "OmsEngine")
_NONDETERMINISTIC_MODULES = {
    "random": "MINIQMT_PLUGIN_IMPORT_GLOBAL_RANDOM_FORBIDDEN",
    "secrets": "MINIQMT_PLUGIN_IMPORT_GLOBAL_RANDOM_FORBIDDEN",
    "time": "MINIQMT_PLUGIN_IMPORT_WALL_CLOCK_FORBIDDEN",
    "uuid": "MINIQMT_PLUGIN_IMPORT_UUID_FORBIDDEN",
}
_WALL_CLOCK_CALLS = frozenset(
    {
        "datetime.date.today",
        "datetime.datetime.now",
        "datetime.datetime.today",
        "datetime.datetime.utcnow",
        "time.monotonic",
        "time.monotonic_ns",
        "time.perf_counter",
        "time.perf_counter_ns",
        "time.process_time",
        "time.time",
        "time.time_ns",
    }
)
_DYNAMIC_IMPORT_CALLS = frozenset({"__import__", "importlib.import_module"})
_MAX_CONTEXT_TEXT = 2048
_ISOLATED_IMPORT_TIMEOUT_SECONDS = 20
_MODULE_NAME_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")


def _strict_identity(value: Any, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty, trim-stable string")
    return value


def _freeze_context(value: Any) -> Any:
    if value is None or type(value) in (bool, int, str):
        if type(value) is str and len(value) > _MAX_CONTEXT_TEXT:
            return value[:_MAX_CONTEXT_TEXT] + "..."
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        keys = tuple(value)
        if any(type(key) is not str for key in keys):
            raise TypeError("failure context keys must be strings")
        for key in sorted(keys):
            frozen[key] = _freeze_context(value[key])
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_context(item) for item in value)
    raise TypeError(f"failure context contains unsupported type {type(value).__name__}")


def _thaw_context(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_context(member) for key, member in value.items()}
    if isinstance(value, tuple):
        return [_thaw_context(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class ImportBoundaryTargetV1:
    module_name: str
    source_path: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "module_name", _strict_identity(self.module_name, field_name="module_name"))
        if _MODULE_NAME_RE.fullmatch(self.module_name) is None:
            raise ValueError("module_name must be a dotted Python module name")
        if not isinstance(self.source_path, Path):
            raise TypeError("source_path must be pathlib.Path")
        if self.source_path.suffix != ".py":
            raise ValueError("source_path must identify a Python source file")


@dataclass(frozen=True, slots=True)
class ImportBoundaryFailureV1:
    stage: Literal["SOURCE", "AST", "ISOLATED_IMPORT"]
    module_name: str
    source_path: str
    line: int
    column: int
    reason: str
    context: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.stage not in ("SOURCE", "AST", "ISOLATED_IMPORT"):
            raise ValueError("failure stage is invalid")
        for field_name in ("module_name", "source_path", "reason"):
            object.__setattr__(self, field_name, _strict_identity(getattr(self, field_name), field_name=field_name))
        if type(self.line) is not int or self.line < 0 or type(self.column) is not int or self.column < 0:
            raise ValueError("failure line and column must be non-negative integers")
        object.__setattr__(self, "context", _freeze_context(self.context))

    @property
    def sort_key(self) -> tuple[Any, ...]:
        context_json = json.dumps(
            _thaw_context(self.context),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return (
            {"SOURCE": 0, "AST": 1, "ISOLATED_IMPORT": 2}[self.stage],
            self.module_name,
            self.source_path,
            self.line,
            self.column,
            self.reason,
            context_json,
        )


@dataclass(frozen=True, slots=True)
class PluginImportBoundaryReceiptV1:
    schema_version: Literal["plugin_import_boundary_receipt_v1"]
    status: Literal["PASSED", "FAILED"]
    checked_modules: tuple[str, ...]
    ordered_failures: tuple[ImportBoundaryFailureV1, ...]

    def __post_init__(self) -> None:
        if self.schema_version != "plugin_import_boundary_receipt_v1":
            raise ValueError("import boundary receipt schema_version is invalid")
        if self.status not in ("PASSED", "FAILED"):
            raise ValueError("import boundary receipt status is invalid")
        if type(self.checked_modules) is not tuple or any(type(item) is not str for item in self.checked_modules):
            raise TypeError("checked_modules must be a tuple of strings")
        if type(self.ordered_failures) is not tuple or any(
            type(item) is not ImportBoundaryFailureV1 for item in self.ordered_failures
        ):
            raise TypeError("ordered_failures must be a tuple of ImportBoundaryFailureV1")
        if self.checked_modules != tuple(sorted(set(self.checked_modules))):
            raise ValueError("checked_modules must be unique and canonically sorted")
        expected = tuple(sorted(self.ordered_failures, key=lambda item: item.sort_key))
        if self.ordered_failures != expected:
            raise ValueError("ordered_failures must be canonically sorted")
        if self.status == "PASSED" and self.ordered_failures:
            raise ValueError("PASSED import receipt must not contain failures")
        if self.status == "FAILED" and not self.ordered_failures:
            raise ValueError("FAILED import receipt must contain failures")


class PluginImportBoundaryError(RuntimeError):
    def __init__(self, receipt: PluginImportBoundaryReceiptV1) -> None:
        self.receipt = receipt
        super().__init__(f"plugin import boundary failed with {len(receipt.ordered_failures)} failure(s)")


def _matches_prefix(import_name: str, prefix: str) -> bool:
    return import_name == prefix or import_name.startswith(prefix + ".")


def _forbidden_import_reason(import_name: str) -> str | None:
    root = import_name.split(".", 1)[0]
    if root in _NONDETERMINISTIC_MODULES:
        return _NONDETERMINISTIC_MODULES[root]
    if _matches_prefix(import_name, "backend") and not any(
        _matches_prefix(import_name, prefix) for prefix in _ALLOWED_INTERNAL_IMPORT_PREFIXES
    ):
        return "MINIQMT_PLUGIN_IMPORT_FORBIDDEN_DEPENDENCY"
    if any(_matches_prefix(import_name, prefix) for prefix in _FORBIDDEN_IMPORT_PREFIXES):
        return "MINIQMT_PLUGIN_IMPORT_FORBIDDEN_DEPENDENCY"
    return None


def _failure(
    *,
    stage: Literal["SOURCE", "AST", "ISOLATED_IMPORT"],
    target: ImportBoundaryTargetV1,
    reason: str,
    context: Mapping[str, Any],
    line: int = 0,
    column: int = 0,
) -> ImportBoundaryFailureV1:
    return ImportBoundaryFailureV1(
        stage=stage,
        module_name=target.module_name,
        source_path=target.source_path.as_posix(),
        line=line,
        column=column,
        reason=reason,
        context=context,
    )


class _ImportBoundaryAstVisitor(ast.NodeVisitor):
    def __init__(self, target: ImportBoundaryTargetV1) -> None:
        self._target = target
        self._aliases: dict[str, str] = {}
        self.failures: list[ImportBoundaryFailureV1] = []

    def _append(self, node: ast.AST, *, reason: str, context: Mapping[str, Any]) -> None:
        self.failures.append(
            _failure(
                stage="AST",
                target=self._target,
                reason=reason,
                context=context,
                line=getattr(node, "lineno", 0),
                column=getattr(node, "col_offset", 0),
            )
        )

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        if node.name.endswith(_FORBIDDEN_OWNER_CLASS_SUFFIXES):
            self._append(
                node,
                reason="MINIQMT_PLUGIN_IMPORT_PARALLEL_RUNTIME_OWNER_FORBIDDEN",
                context={"defined_class": node.name},
            )
        self.generic_visit(node)

    def _record_import(self, node: ast.AST, *, import_name: str, local_name: str) -> None:
        self._aliases[local_name] = import_name
        reason = _forbidden_import_reason(import_name)
        if reason is not None:
            self._append(node, reason=reason, context={"import_name": import_name})

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._record_import(
                node,
                import_name=alias.name,
                local_name=alias.asname or alias.name.split(".", 1)[0],
            )
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        import_module = self._resolve_import_from(node)
        if import_module:
            self._record_import(node, import_name=import_module, local_name=import_module.split(".", 1)[0])
            for alias in node.names:
                symbol_name = alias.name
                full_name = f"{import_module}.{symbol_name}"
                self._aliases[alias.asname or symbol_name] = full_name
                reason = _forbidden_import_reason(full_name)
                if reason is not None and _forbidden_import_reason(import_module) is None:
                    self._append(node, reason=reason, context={"import_name": full_name})
                if symbol_name in _FORBIDDEN_OWNER_SYMBOLS:
                    self._append(
                        node,
                        reason="MINIQMT_PLUGIN_IMPORT_PARALLEL_RUNTIME_OWNER_FORBIDDEN",
                        context={"import_name": full_name, "symbol": symbol_name},
                    )
        self.generic_visit(node)

    def _resolve_import_from(self, node: ast.ImportFrom) -> str | None:
        if node.level == 0:
            return node.module
        package_parts = self._target.module_name.split(".")[:-1]
        parent_steps = node.level - 1
        if parent_steps > len(package_parts):
            self._append(
                node,
                reason="MINIQMT_PLUGIN_IMPORT_RELATIVE_LEVEL_INVALID",
                context={"level": node.level, "module": node.module},
            )
            return None
        base_parts = package_parts[: len(package_parts) - parent_steps]
        if node.module:
            base_parts.extend(node.module.split("."))
        return ".".join(base_parts)

    def _call_name(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return self._aliases.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            prefix = self._call_name(node.value)
            return f"{prefix}.{node.attr}" if prefix else None
        return None

    def visit_Assign(self, node: ast.Assign) -> None:
        aliased_name = self._call_name(node.value)
        if aliased_name:
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self._aliases[target.id] = aliased_name
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.value is not None:
            aliased_name = self._call_name(node.value)
            if aliased_name:
                self._aliases[node.target.id] = aliased_name
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        call_name = self._call_name(node.func)
        if call_name in _WALL_CLOCK_CALLS:
            self._append(
                node,
                reason="MINIQMT_PLUGIN_IMPORT_WALL_CLOCK_FORBIDDEN",
                context={"call": call_name},
            )
        elif call_name in _DYNAMIC_IMPORT_CALLS:
            self._append(
                node,
                reason="MINIQMT_PLUGIN_IMPORT_DYNAMIC_IMPORT_FORBIDDEN",
                context={"call": call_name},
            )
        elif call_name in {"os.getenv", "os.open", "os.popen", "os.system"} or (
            call_name is not None and call_name.startswith("os.environ.")
        ):
            self._append(
                node,
                reason="MINIQMT_PLUGIN_IMPORT_EXTERNAL_STATE_FORBIDDEN",
                context={"call": call_name},
            )
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if self._call_name(node.value) == "os.environ":
            self._append(
                node,
                reason="MINIQMT_PLUGIN_IMPORT_EXTERNAL_STATE_FORBIDDEN",
                context={"access": "os.environ"},
            )
        self.generic_visit(node)


_ISOLATED_IMPORT_SCRIPT = r"""
import asyncio
import builtins
import importlib.util
import json
import multiprocessing
import os
import pathlib
import socket
import subprocess
import sys
import threading
import types
import _thread

# Preload the required K1-A dependencies before side-effect guards. Missing
# dependencies fail the isolated process instead of being silently ignored.
from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

module_name, source_path, repo_root = sys.argv[1:4]
events = []
target_source = os.path.normcase(os.path.abspath(source_path))

class BoundarySideEffect(RuntimeError):
    pass

def guard(operation, original):
    def guarded(*args, **kwargs):
        caller_source = os.path.normcase(os.path.abspath(sys._getframe(1).f_code.co_filename))
        if caller_source == target_source:
            events.append({"operation": operation})
            raise BoundarySideEffect(operation)
        return original(*args, **kwargs)
    return guarded

def guarded_open(file, mode="r", *args, **kwargs):
    operation = "open_write" if any(flag in mode for flag in "wax+") else "open_read"
    caller_source = os.path.normcase(os.path.abspath(sys._getframe(1).f_code.co_filename))
    if caller_source == target_source:
        events.append({"operation": operation})
        raise BoundarySideEffect(operation)
    return original_open(file, mode, *args, **kwargs)

original_open = builtins.open
originals = []
def patch(owner, name, operation=None, replacement=None):
    if hasattr(owner, name):
        original = getattr(owner, name)
        originals.append((owner, name, original))
        setattr(owner, name, replacement or guard(operation, original))

patch(builtins, "open", replacement=guarded_open)
for name in ("open", "read_bytes", "read_text", "write_bytes", "write_text", "touch", "mkdir", "unlink", "rename", "replace"):
    patch(pathlib.Path, name, "pathlib_" + name)
patch(os, "open", "os_open")
patch(os, "popen", "os_popen")
patch(os, "system", "os_system")
patch(socket, "socket", "socket")
patch(socket, "create_connection", "socket_create_connection")
for name in ("Popen", "run", "call", "check_call", "check_output"):
    patch(subprocess, name, "subprocess_" + name)
patch(threading.Thread, "start", "thread_start")
patch(multiprocessing.Process, "start", "process_start")
patch(_thread, "start_new_thread", "thread_start_new")
patch(asyncio, "create_task", "asyncio_create_task")
patch(asyncio, "ensure_future", "asyncio_ensure_future")

parts = module_name.split(".")
for index in range(1, len(parts)):
    package_name = ".".join(parts[:index])
    if package_name in sys.modules:
        continue
    package = types.ModuleType(package_name)
    package.__package__ = package_name
    package.__path__ = [os.path.join(repo_root, *parts[:index])]
    package.__file__ = os.path.join(package.__path__[0], "__init__.py")
    sys.modules[package_name] = package

result = {"events": events, "exception": None}
try:
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise ImportError("source loader unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
except Exception as exc:
    try:
        message = str(exc)[:2048]
        message_render_error_type = None
    except Exception as render_error:
        message = "<unavailable>"
        message_render_error_type = type(render_error).__module__ + "." + type(render_error).__qualname__
    result["exception"] = {
        "type": type(exc).__module__ + "." + type(exc).__qualname__,
        "message": message,
        "message_render_error_type": message_render_error_type,
    }
finally:
    for owner, name, original in reversed(originals):
        setattr(owner, name, original)

sys.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
"""


def _isolated_import_failures(
    *, target: ImportBoundaryTargetV1, repo_root: Path
) -> tuple[ImportBoundaryFailureV1, ...]:
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                _ISOLATED_IMPORT_SCRIPT,
                target.module_name,
                str(target.source_path),
                str(repo_root),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=_ISOLATED_IMPORT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return (
            _failure(
                stage="ISOLATED_IMPORT",
                target=target,
                reason="MINIQMT_PLUGIN_IMPORT_ISOLATED_TIMEOUT",
                context={"timeout_seconds": _ISOLATED_IMPORT_TIMEOUT_SECONDS},
            ),
        )
    if completed.returncode != 0:
        return (
            _failure(
                stage="ISOLATED_IMPORT",
                target=target,
                reason="MINIQMT_PLUGIN_IMPORT_ISOLATED_PROCESS_FAILED",
                context={
                    "returncode": completed.returncode,
                    "stderr": completed.stderr[:_MAX_CONTEXT_TEXT],
                },
            ),
        )
    try:
        result = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        return (
            _failure(
                stage="ISOLATED_IMPORT",
                target=target,
                reason="MINIQMT_PLUGIN_IMPORT_ISOLATED_RECEIPT_INVALID",
                context={"error_type": type(exc).__name__, "stdout": completed.stdout[:_MAX_CONTEXT_TEXT]},
            ),
        )
    failures: list[ImportBoundaryFailureV1] = []
    for event in result.get("events", []):
        operation = event.get("operation") if isinstance(event, dict) else None
        failures.append(
            _failure(
                stage="ISOLATED_IMPORT",
                target=target,
                reason="MINIQMT_PLUGIN_IMPORT_SIDE_EFFECT_FORBIDDEN",
                context={"operation": operation or "unknown"},
            )
        )
    exception = result.get("exception")
    if isinstance(exception, dict):
        failures.append(
            _failure(
                stage="ISOLATED_IMPORT",
                target=target,
                reason="MINIQMT_PLUGIN_IMPORT_EXECUTION_FAILED",
                context={
                    "exception_type": exception.get("type", "unknown"),
                    "message": exception.get("message", "unavailable"),
                    "message_render_error_type": exception.get("message_render_error_type"),
                },
            )
        )
    return tuple(failures)


def validate_plugin_import_boundaries_v1(
    *,
    repo_root: Path,
    targets: tuple[ImportBoundaryTargetV1, ...],
) -> PluginImportBoundaryReceiptV1:
    """Validate K1 imports without loading product package initializers."""

    if not isinstance(repo_root, Path) or type(targets) is not tuple:
        raise TypeError("repo_root must be Path and targets must be tuple")
    root = repo_root.resolve(strict=True)
    if not targets:
        raise ValueError("targets must not be empty")
    ordered_targets = tuple(sorted(targets, key=lambda item: (item.module_name, item.source_path.as_posix())))
    if len({target.module_name for target in ordered_targets}) != len(ordered_targets):
        raise ValueError("target module names must be unique")
    if len({target.source_path.resolve(strict=False) for target in ordered_targets}) != len(ordered_targets):
        raise ValueError("target source paths must be unique")

    failures: list[ImportBoundaryFailureV1] = []
    ast_valid_targets: list[ImportBoundaryTargetV1] = []
    for target in ordered_targets:
        candidate_path = target.source_path.resolve(strict=False)
        if not candidate_path.is_relative_to(root):
            raise ValueError(f"target {target.module_name} must be inside repo_root")
        try:
            resolved_path = target.source_path.resolve(strict=True)
        except OSError as exc:
            failures.append(
                _failure(
                    stage="SOURCE",
                    target=target,
                    reason="MINIQMT_PLUGIN_IMPORT_SOURCE_UNAVAILABLE",
                    context={"error_type": type(exc).__name__},
                )
            )
            continue
        try:
            source = resolved_path.read_text(encoding="utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            failures.append(
                _failure(
                    stage="SOURCE",
                    target=target,
                    reason="MINIQMT_PLUGIN_IMPORT_SOURCE_DECODE_INVALID",
                    context={"decode_error_type": type(exc).__name__, "start": exc.start, "end": exc.end},
                )
            )
            continue
        except OSError as exc:
            failures.append(
                _failure(
                    stage="SOURCE",
                    target=target,
                    reason="MINIQMT_PLUGIN_IMPORT_SOURCE_UNAVAILABLE",
                    context={"error_type": type(exc).__name__},
                )
            )
            continue
        try:
            tree = ast.parse(source, filename=target.source_path.as_posix(), mode="exec", type_comments=True)
        except SyntaxError as exc:
            failures.append(
                _failure(
                    stage="SOURCE",
                    target=target,
                    reason="MINIQMT_PLUGIN_IMPORT_SOURCE_AST_INVALID",
                    context={"syntax_error": exc.msg},
                    line=exc.lineno or 0,
                    column=exc.offset or 0,
                )
            )
            continue
        visitor = _ImportBoundaryAstVisitor(target)
        visitor.visit(tree)
        if visitor.failures:
            failures.extend(visitor.failures)
            continue
        ast_valid_targets.append(target)

    for target in ast_valid_targets:
        failures.extend(_isolated_import_failures(target=target, repo_root=root))

    ordered_failures = tuple(sorted(failures, key=lambda item: item.sort_key))
    receipt = PluginImportBoundaryReceiptV1(
        schema_version="plugin_import_boundary_receipt_v1",
        status="FAILED" if ordered_failures else "PASSED",
        checked_modules=tuple(target.module_name for target in ordered_targets),
        ordered_failures=ordered_failures,
    )
    if ordered_failures:
        raise PluginImportBoundaryError(receipt)
    return receipt


__all__ = [
    "ImportBoundaryFailureV1",
    "ImportBoundaryTargetV1",
    "PluginImportBoundaryError",
    "PluginImportBoundaryReceiptV1",
    "validate_plugin_import_boundaries_v1",
]
