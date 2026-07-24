from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.execution_algos.vnpy_compat.import_boundary import (
    ImportBoundaryTargetV1,
    PluginImportBoundaryError,
    validate_plugin_import_boundaries_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


def _target(module_name: str, path: Path) -> ImportBoundaryTargetV1:
    return ImportBoundaryTargetV1(module_name=module_name, source_path=path)


def _write_source(tmp_path: Path, module_name: str, source: str) -> ImportBoundaryTargetV1:
    path = tmp_path.joinpath(*module_name.split(".")).with_suffix(".py")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    return _target(module_name, path)


def test_real_k1_a_and_k1_c_modules_pass_ast_and_isolated_import() -> None:
    targets = (
        _target(
            "backend.services.miniqmt_execution_runtime.plugin_canonical",
            REPO_ROOT / "backend/services/miniqmt_execution_runtime/plugin_canonical.py",
        ),
        _target(
            "backend.services.miniqmt_execution_runtime.plugin_contracts",
            REPO_ROOT / "backend/services/miniqmt_execution_runtime/plugin_contracts.py",
        ),
        _target(
            "backend.services.miniqmt_execution_runtime.deterministic_context",
            REPO_ROOT / "backend/services/miniqmt_execution_runtime/deterministic_context.py",
        ),
        _target(
            "backend.execution_algos.vnpy_compat.import_boundary",
            REPO_ROOT / "backend/execution_algos/vnpy_compat/import_boundary.py",
        ),
        _target(
            "backend.execution_algos.vnpy_compat.__init__",
            REPO_ROOT / "backend/execution_algos/vnpy_compat/__init__.py",
        ),
        _target(
            "backend.execution_algos.vnpy_compat.locked_surface",
            REPO_ROOT / "backend/execution_algos/vnpy_compat/locked_surface.py",
        ),
        _target(
            "backend.execution_algos.vnpy_compat.receipts",
            REPO_ROOT / "backend/execution_algos/vnpy_compat/receipts.py",
        ),
    )

    receipt = validate_plugin_import_boundaries_v1(repo_root=REPO_ROOT, targets=targets)

    assert receipt.status == "PASSED"
    assert receipt.checked_modules == tuple(sorted(target.module_name for target in targets))
    assert receipt.ordered_failures == ()


@pytest.mark.parametrize(
    ("source", "expected_import"),
    [
        ("import requests as transport\n", "requests"),
        ("from backend.db import pg_pool as storage\n", "backend.db"),
        ("from backend.infra.qmt_client import QmtClient\n", "backend.infra.qmt_client"),
        ("import backend.services.simulation_runtime.scheduler\n", "backend.services.simulation_runtime.scheduler"),
        ("from backend.services.selection import service\n", "backend.services.selection"),
        ("from sqlalchemy.orm import Session\n", "sqlalchemy.orm"),
        ("import xtquant.xttrader as trader\n", "xtquant.xttrader"),
        ("import socket\n", "socket"),
    ],
)
def test_ast_rejects_direct_alias_from_and_submodule_imports(
    tmp_path: Path,
    source: str,
    expected_import: str,
) -> None:
    target = _write_source(tmp_path, "fixtures.forbidden_import", source)

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    failure = exc_info.value.receipt.ordered_failures[0]
    assert failure.reason == "MINIQMT_PLUGIN_IMPORT_FORBIDDEN_DEPENDENCY"
    assert failure.context["import_name"] == expected_import
    assert failure.module_name == target.module_name
    assert failure.source_path == "fixtures/forbidden_import.py"


@pytest.mark.parametrize(
    ("source", "reason"),
    [
        ("import random as entropy\nentropy.random()\n", "MINIQMT_PLUGIN_IMPORT_GLOBAL_RANDOM_FORBIDDEN"),
        ("from uuid import uuid4 as new_id\nnew_id()\n", "MINIQMT_PLUGIN_IMPORT_UUID_FORBIDDEN"),
        (
            "from datetime import datetime as Clock\nClock.now()\n",
            "MINIQMT_PLUGIN_IMPORT_WALL_CLOCK_FORBIDDEN",
        ),
        ("import time as clock\nclock.time()\n", "MINIQMT_PLUGIN_IMPORT_WALL_CLOCK_FORBIDDEN"),
    ],
)
def test_ast_rejects_nondeterministic_imports_and_calls(tmp_path: Path, source: str, reason: str) -> None:
    target = _write_source(tmp_path, "fixtures.nondeterministic", source)

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert reason in {failure.reason for failure in exc_info.value.receipt.ordered_failures}


def test_ast_rejects_relative_import_escape_and_second_order_wall_clock_alias(tmp_path: Path) -> None:
    relative_target = _write_source(
        tmp_path,
        "backend.execution_algos.vnpy_compat.relative_escape",
        "from ...services.simulation_runtime import scheduler\n",
    )
    wall_clock_target = _write_source(
        tmp_path,
        "fixtures.wall_clock_alias",
        "from datetime import datetime as Clock\nclock_now = Clock.now\nclock_now()\n",
    )

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(
            repo_root=tmp_path,
            targets=(wall_clock_target, relative_target),
        )

    assert {item.reason for item in exc_info.value.receipt.ordered_failures} == {
        "MINIQMT_PLUGIN_IMPORT_FORBIDDEN_DEPENDENCY",
        "MINIQMT_PLUGIN_IMPORT_WALL_CLOCK_FORBIDDEN",
    }


@pytest.mark.parametrize(
    "source",
    [
        "try:\n    import httpx\nexcept Exception:\n    pass\n",
        "if False:\n    from backend.repositories import orders\n",
    ],
)
def test_ast_does_not_allow_try_or_conditional_imports_to_mask_forbidden_dependencies(
    tmp_path: Path,
    source: str,
) -> None:
    target = _write_source(tmp_path, "fixtures.masked_import", source)

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert any(
        failure.reason == "MINIQMT_PLUGIN_IMPORT_FORBIDDEN_DEPENDENCY"
        for failure in exc_info.value.receipt.ordered_failures
    )


@pytest.mark.parametrize("class_name", ["EventEngine", "LocalOmsEngine", "CustomGateway"])
def test_ast_rejects_parallel_runtime_owner_definitions(tmp_path: Path, class_name: str) -> None:
    target = _write_source(tmp_path, "fixtures.parallel_owner", f"class {class_name}:\n    pass\n")

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    failure = exc_info.value.receipt.ordered_failures[0]
    assert failure.reason == "MINIQMT_PLUGIN_IMPORT_PARALLEL_RUNTIME_OWNER_FORBIDDEN"
    assert failure.context["defined_class"] == class_name


def test_ast_rejects_environment_reads_through_aliases(tmp_path: Path) -> None:
    target = _write_source(
        tmp_path,
        "fixtures.environment_read",
        "from os import environ as config\nVALUE = config['SECRET']\n",
    )

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert exc_info.value.receipt.ordered_failures[0].reason == "MINIQMT_PLUGIN_IMPORT_EXTERNAL_STATE_FORBIDDEN"


def test_isolated_import_rejects_caught_file_write_side_effect(tmp_path: Path) -> None:
    marker = tmp_path / "import-side-effect.txt"
    target = _write_source(
        tmp_path,
        "fixtures.file_side_effect",
        (
            "try:\n"
            f"    open({str(marker)!r}, 'w', encoding='utf-8').write('forbidden')\n"
            "except RuntimeError:\n"
            "    pass\n"
        ),
    )

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert not marker.exists()
    failure = exc_info.value.receipt.ordered_failures[0]
    assert failure.reason == "MINIQMT_PLUGIN_IMPORT_FILESYSTEM_SIDE_EFFECT_FORBIDDEN"
    assert failure.stage == "AST"
    assert failure.context["call"] == "open"


def test_isolated_import_does_not_execute_parent_package_init(tmp_path: Path) -> None:
    marker = tmp_path / "parent-init-ran.txt"
    package = tmp_path / "isolated_pkg"
    package.mkdir()
    (package / "__init__.py").write_text(
        f"open({str(marker)!r}, 'w', encoding='utf-8').write('unexpected')\n",
        encoding="utf-8",
    )
    target_path = package / "target.py"
    target_path.write_text("VALUE = 7\n", encoding="utf-8")

    receipt = validate_plugin_import_boundaries_v1(
        repo_root=tmp_path,
        targets=(_target("isolated_pkg.target", target_path),),
    )

    assert receipt.status == "PASSED"
    assert not marker.exists()


def test_isolated_import_preserves_primary_failure_when_exception_renderer_breaks(tmp_path: Path) -> None:
    target = _write_source(
        tmp_path,
        "fixtures.broken_exception_renderer",
        (
            "class BrokenError(RuntimeError):\n"
            "    def __str__(self):\n"
            "        raise LookupError('renderer failed')\n"
            "raise BrokenError()\n"
        ),
    )

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    failure = exc_info.value.receipt.ordered_failures[0]
    assert failure.reason == "MINIQMT_PLUGIN_IMPORT_EXECUTION_FAILED"
    assert failure.context["exception_type"].endswith(".BrokenError")
    assert failure.context["message"] == "<unavailable>"
    assert failure.context["message_render_error_type"] == "builtins.LookupError"


def test_decode_and_ast_failures_are_aggregated_in_stable_order(tmp_path: Path) -> None:
    decode_path = tmp_path / "fixtures" / "decode_failure.py"
    decode_path.parent.mkdir(parents=True)
    decode_path.write_bytes(b"\xff\xfe\x00")
    syntax_target = _write_source(tmp_path, "fixtures.syntax_failure", "def broken(:\n")
    decode_target = _target("fixtures.decode_failure", decode_path)

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(
            repo_root=tmp_path,
            targets=(syntax_target, decode_target),
        )

    receipt = exc_info.value.receipt
    assert tuple(item.reason for item in receipt.ordered_failures) == (
        "MINIQMT_PLUGIN_IMPORT_SOURCE_DECODE_INVALID",
        "MINIQMT_PLUGIN_IMPORT_SOURCE_AST_INVALID",
    )
    assert receipt.ordered_failures == tuple(sorted(receipt.ordered_failures, key=lambda item: item.sort_key))
    with pytest.raises(TypeError):
        receipt.ordered_failures[0].context["new"] = "mutation"


def test_target_rejects_paths_outside_repo_root(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside_import_boundary.py"
    outside.write_text("VALUE = 1\n", encoding="utf-8")
    target = _target("outside_import_boundary", outside)

    with pytest.raises(ValueError, match="inside repo_root"):
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    missing_outside = _target("outside_missing", tmp_path.parent / "outside_missing.py")
    with pytest.raises(ValueError, match="inside repo_root"):
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(missing_outside,))


def test_missing_source_is_an_aggregate_failure_with_module_and_path(tmp_path: Path) -> None:
    target = _target("fixtures.missing", tmp_path / "fixtures/missing.py")

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    failure = exc_info.value.receipt.ordered_failures[0]
    assert failure.reason == "MINIQMT_PLUGIN_IMPORT_SOURCE_UNAVAILABLE"
    assert failure.module_name == target.module_name
    assert failure.source_path == "fixtures/missing.py"
    assert failure.context["error_type"] == "FileNotFoundError"


def test_duplicate_source_target_is_rejected_before_execution(tmp_path: Path) -> None:
    path = tmp_path / "shared.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="source paths must be unique"):
        validate_plugin_import_boundaries_v1(
            repo_root=tmp_path,
            targets=(_target("fixtures.one", path), _target("fixtures.two", path)),
        )


@pytest.mark.parametrize(
    ("source", "expected_reason"),
    [
        (
            'import importlib\ngetattr(importlib, "import_module")("backend.services.simulation_runtime")\n',
            "MINIQMT_PLUGIN_IMPORT_DYNAMIC_IMPORT_FORBIDDEN",
        ),
        (
            'import builtins\nbuiltins.__import__("backend.db")\n',
            "MINIQMT_PLUGIN_IMPORT_DYNAMIC_IMPORT_FORBIDDEN",
        ),
        (
            'import os\nVALUE = getattr(os, "environ").get("SECRET")\n',
            "MINIQMT_PLUGIN_IMPORT_EXTERNAL_STATE_FORBIDDEN",
        ),
        (
            'import importlib\nlookup = getattr\nattribute = "import_module"\n'
            'lookup(importlib, attribute)("backend.services.simulation_runtime")\n',
            "MINIQMT_PLUGIN_IMPORT_DYNAMIC_ATTRIBUTE_ESCAPE_FORBIDDEN",
        ),
    ],
)
def test_public_validator_rejects_indirect_dynamic_import_and_environment_access(
    tmp_path: Path,
    source: str,
    expected_reason: str,
) -> None:
    target = _write_source(tmp_path, "fixtures.indirect_escape", source)

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert expected_reason in {item.reason for item in exc_info.value.receipt.ordered_failures}


@pytest.mark.parametrize(
    "source_factory",
    [
        lambda marker: f"import io\nio.open({str(marker)!r}, 'w', encoding='utf-8').write('forbidden')\n",
        lambda marker: f"import os\nos.remove({str(marker)!r})\n",
        lambda marker: f"import os\nos.unlink({str(marker)!r})\n",
    ],
)
def test_public_validator_blocks_io_and_os_file_mutation_before_it_occurs(
    tmp_path: Path,
    source_factory,
) -> None:
    marker = tmp_path / "protected.txt"
    marker.write_text("original", encoding="utf-8")
    source = source_factory(marker)
    target = _write_source(tmp_path, "fixtures.file_escape", source)

    with pytest.raises(PluginImportBoundaryError):
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert marker.read_text(encoding="utf-8") == "original"


def test_public_validator_blocks_helper_mediated_file_side_effect(tmp_path: Path) -> None:
    marker = tmp_path / "helper-created.txt"
    package = tmp_path / "fixtures"
    package.mkdir(parents=True, exist_ok=True)
    (package / "side_helper.py").write_text(
        f"open({str(marker)!r}, 'w', encoding='utf-8').write('forbidden')\n",
        encoding="utf-8",
    )
    target = _write_source(tmp_path, "fixtures.helper_target", "from . import side_helper\n")

    with pytest.raises(PluginImportBoundaryError):
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert not marker.exists()


@pytest.mark.parametrize(
    "helper_source",
    [
        (
            "import importlib\n"
            'importlib.import_module("socket")\n'
            "open({marker!r}, 'w', encoding='utf-8').write('dynamic-import-ran')\n"
        ),
        (
            "import sys\n"
            'sys.modules["socket"].socket()\n'
            "open({marker!r}, 'w', encoding='utf-8').write('network-ran')\n"
        ),
        (
            "import sys\n"
            'sys.modules["subprocess"].run('
            "[sys.executable, \"-c\", \"open({marker!r}, 'w').write('process-ran')\"], check=True)\n"
        ),
        (
            "import sys\n"
            'threading = sys.modules["threading"]\n'
            "worker = threading.Thread(target=lambda: open({marker!r}, 'w').write('thread-ran'))\n"
            "worker.start()\nworker.join()\n"
        ),
        (
            "import sys\n"
            'sys.modules["os"].environ.get("SECRET")\n'
            "open({marker!r}, 'w', encoding='utf-8').write('environment-ran')\n"
        ),
    ],
)
def test_public_validator_blocks_helper_mediated_external_side_effects(
    tmp_path: Path,
    helper_source: str,
) -> None:
    marker = tmp_path / "helper-external-side-effect.txt"
    package = tmp_path / "fixtures"
    package.mkdir(parents=True, exist_ok=True)
    (package / "external_helper.py").write_text(
        helper_source.format(marker=str(marker)),
        encoding="utf-8",
    )
    target = _write_source(tmp_path, "fixtures.external_target", "from . import external_helper\n")

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert any(
        item.reason == "MINIQMT_PLUGIN_IMPORT_SIDE_EFFECT_FORBIDDEN" for item in exc_info.value.receipt.ordered_failures
    )
    assert not marker.exists()


def test_repo_helper_dynamic_import_retains_exact_forbidden_module(tmp_path: Path) -> None:
    _write_source(
        tmp_path,
        "fixtures.dynamic_helper",
        'import importlib\nimportlib.import_module("backend.services.simulation_runtime")\n',
    )
    target = _write_source(tmp_path, "fixtures.dynamic_target", "from . import dynamic_helper\n")

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    failures = exc_info.value.receipt.ordered_failures
    assert any(
        failure.reason == "MINIQMT_PLUGIN_IMPORT_SIDE_EFFECT_FORBIDDEN"
        and failure.context.get("operation") == "dynamic_import"
        and failure.context.get("module") == "backend.services.simulation_runtime"
        for failure in failures
    )


def test_external_validation_dependency_internal_import_is_not_repo_attributed(tmp_path: Path) -> None:
    target = _write_source(
        tmp_path,
        "fixtures.validation_dependency",
        "from jsonschema.validators import validator_for\n"
        "from pydantic import BaseModel\n"
        "assert validator_for is not None and BaseModel is not None\n",
    )

    receipt = validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert receipt.status == "PASSED"


def test_import_failure_identity_is_repo_relative_and_root_independent(tmp_path: Path) -> None:
    failures = []
    for root_name in ("checkout-a", "checkout-b"):
        root = tmp_path / root_name
        target = _write_source(root, "fixtures.forbidden", "import backend.db\n")
        with pytest.raises(PluginImportBoundaryError) as exc_info:
            validate_plugin_import_boundaries_v1(repo_root=root, targets=(target,))
        failures.append(exc_info.value.receipt.ordered_failures[0])

    assert failures[0].source_path == "fixtures/forbidden.py"
    assert failures[0].sort_key == failures[1].sort_key


def test_isolated_import_failure_context_is_repo_relative_and_root_independent(tmp_path: Path) -> None:
    failures = []
    error_messages = []
    for root_name in ("checkout-a", "checkout-b"):
        root = tmp_path / root_name
        target = _write_source(root, "fixtures.runtime_failure", "raise RuntimeError(__file__)\n")
        with pytest.raises(PluginImportBoundaryError) as exc_info:
            validate_plugin_import_boundaries_v1(repo_root=root, targets=(target,))
        failures.append(exc_info.value.receipt.ordered_failures[0])
        error_messages.append(str(exc_info.value))

    assert failures[0].source_path == "fixtures/runtime_failure.py"
    assert failures[0].sort_key == failures[1].sort_key
    assert error_messages[0] == error_messages[1]
    assert "MINIQMT_PLUGIN_IMPORT_EXECUTION_FAILED" in error_messages[0]
    assert "fixtures/runtime_failure.py" in error_messages[0]


@pytest.mark.parametrize("mode", ["r", "w"])
def test_isolated_import_guard_blocks_raw_fileio_access(tmp_path: Path, mode: str) -> None:
    marker = tmp_path / "raw-fileio.txt"
    marker.write_bytes(b"original")
    target = _write_source(
        tmp_path,
        "fixtures.raw_fileio",
        f"import _io\n_io.FileIO({str(marker)!r}, {mode!r})\n",
    )

    with pytest.raises(PluginImportBoundaryError):
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert marker.read_bytes() == b"original"


def test_isolated_import_guard_blocks_helper_mediated_raw_fileio_write(tmp_path: Path) -> None:
    marker = tmp_path / "helper-raw-fileio.txt"
    marker.write_bytes(b"original")
    _write_source(
        tmp_path,
        "fixtures.raw_fileio_helper",
        f"import _io\n_io.FileIO({str(marker)!r}, 'w').write(b'forbidden')\n",
    )
    target = _write_source(tmp_path, "fixtures.raw_fileio_target", "from . import raw_fileio_helper\n")

    with pytest.raises(PluginImportBoundaryError):
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    assert marker.read_bytes() == b"original"


def test_import_failures_are_bounded_truncated_and_hash_closed(tmp_path: Path) -> None:
    source = "".join(f"import backend.db.forbidden_{index}\n" for index in range(100))
    target = _write_source(tmp_path, "fixtures.many_violations", source)

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    receipt = exc_info.value.receipt
    assert receipt.failures_truncated is True
    assert receipt.observed_failure_count == 100
    assert receipt.retained_failure_count == 64
    assert receipt.omitted_failure_count == 36
    assert len(receipt.ordered_failures) == 65
    assert receipt.ordered_failures[-1].source_path == "__failure_set__"
    assert receipt.failure_set_sha256 != "0" * 64
    assert receipt.receipt_sha256 != "0" * 64
    message = str(exc_info.value)
    assert '"observed":100' in message
    assert '"retained":64' in message
    assert '"diagnostic_failures_shown":16' in message
    assert receipt.failure_set_sha256 in message
    assert len(message) < 16_384


@pytest.mark.parametrize(
    "carrier",
    (
        "[]",
        '{"events":{},"exception":null}',
        '{"events":["not-an-object"],"exception":null}',
        '{"events":[],"exception":"not-an-object"}',
    ),
)
def test_public_validator_fails_loud_on_malformed_isolated_carrier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    carrier: str,
) -> None:
    target = _write_source(tmp_path, "fixtures.valid", "VALUE = 1\n")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=carrier, stderr=""),
    )

    with pytest.raises(PluginImportBoundaryError) as exc_info:
        validate_plugin_import_boundaries_v1(repo_root=tmp_path, targets=(target,))

    failure = exc_info.value.receipt.ordered_failures[0]
    assert failure.reason == "MINIQMT_PLUGIN_IMPORT_ISOLATED_RECEIPT_INVALID"
    assert failure.context["error_type"] == "IsolatedImportReceiptShapeError"


@pytest.mark.parametrize("module_name", ["fixtures.invalid-name", "fixtures/path", " fixtures.name"])
def test_target_rejects_invalid_module_identity(tmp_path: Path, module_name: str) -> None:
    with pytest.raises(ValueError, match="module_name"):
        _target(module_name, tmp_path / "source.py")


def test_standard_package_import_has_no_parent_registration_or_legacy_side_effect() -> None:
    script = """
import builtins
import json
import socket
import subprocess
import sys
import threading
sys.path.insert(0, sys.argv[1])
sys.dont_write_bytecode = True
side_effects = []
original_open = builtins.open
def guarded_open(file, mode="r", *args, **kwargs):
    if any(flag in mode for flag in ("w", "a", "x", "+")):
        side_effects.append("open_write")
        raise RuntimeError("package import attempted a write")
    return original_open(file, mode, *args, **kwargs)
def blocked(operation):
    def reject(*args, **kwargs):
        side_effects.append(operation)
        raise RuntimeError(f"package import attempted {operation}")
    return reject
builtins.open = guarded_open
socket.socket.connect = blocked("network_connect")
subprocess.Popen = blocked("process_start")
threading.Thread.start = blocked("thread_start")
import backend.execution_algos.vnpy_compat
registry = sys.modules.get("backend.execution_algos.registry")
payload = {
    "registered_algorithms": len(registry.ALGO_REGISTRY) if registry is not None else 0,
    "legacy_adapter_loaded": "backend.execution_algos.vnpy_style.legacy_adapter" in sys.modules,
    "vnpy_compat_loaded": "backend.execution_algos.vnpy_compat" in sys.modules,
    "runtime_loaded": "backend.services.miniqmt_execution_runtime.runtime" in sys.modules,
    "repository_loaded": "backend.services.miniqmt_execution_runtime.repository" in sys.modules,
    "gateway_loaded": "backend.services.miniqmt_execution_runtime.gateway" in sys.modules,
    "side_effects": side_effects,
}
print(json.dumps(payload, sort_keys=True))
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(REPO_ROOT)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    result = json.loads(completed.stdout)
    assert result == {
        "legacy_adapter_loaded": False,
        "registered_algorithms": 0,
        "vnpy_compat_loaded": True,
        "runtime_loaded": False,
        "repository_loaded": False,
        "gateway_loaded": False,
        "side_effects": [],
    }


def test_explicit_parent_registry_export_preserves_registered_algorithms() -> None:
    script = """
import json
import sys
sys.path.insert(0, sys.argv[1])
from backend.execution_algos import ALGO_REGISTRY, get_algo
print(json.dumps({"count": len(ALGO_REGISTRY), "twap": get_algo("TWAP") is not None}, sort_keys=True))
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(REPO_ROOT)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    result = json.loads(completed.stdout)
    assert result["count"] >= 14
    assert result["twap"] is True
