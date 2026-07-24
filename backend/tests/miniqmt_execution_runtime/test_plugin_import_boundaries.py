from __future__ import annotations

from pathlib import Path

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
    assert failure.source_path == target.source_path.as_posix()


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
    failure = next(
        item
        for item in exc_info.value.receipt.ordered_failures
        if item.reason == "MINIQMT_PLUGIN_IMPORT_SIDE_EFFECT_FORBIDDEN"
    )
    assert failure.stage == "ISOLATED_IMPORT"
    assert failure.context["operation"] == "open_write"


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
    assert failure.source_path == target.source_path.as_posix()
    assert failure.context["error_type"] == "FileNotFoundError"


def test_duplicate_source_target_is_rejected_before_execution(tmp_path: Path) -> None:
    path = tmp_path / "shared.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="source paths must be unique"):
        validate_plugin_import_boundaries_v1(
            repo_root=tmp_path,
            targets=(_target("fixtures.one", path), _target("fixtures.two", path)),
        )


@pytest.mark.parametrize("module_name", ["fixtures.invalid-name", "fixtures/path", " fixtures.name"])
def test_target_rejects_invalid_module_identity(tmp_path: Path, module_name: str) -> None:
    with pytest.raises(ValueError, match="module_name"):
        _target(module_name, tmp_path / "source.py")
