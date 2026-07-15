from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]
SERVICE_ROOT = ROOT / "backend" / "services" / "advisory_phase1"

PRODUCTION_FILES = (
    "phase1g_transactional_writer.py",
    "observation_capture_postgres.py",
)

PRIMITIVES = {
    "control_binding.py": {
        "current_in_transaction",
        "read_exact_in_transaction",
        "append_in_transaction",
        "get_or_append_exact_in_transaction",
    },
    "capture_foundation.py": {
        "lock_running_in_transaction",
        "read_plan_exact_in_transaction",
        "read_memberships_exact_in_transaction",
        "read_memberships_exact_readonly",
        "add_membership_in_transaction",
        "complete_in_transaction",
    },
    "trace_outbox.py": {
        "read_exact_by_hash_in_transaction",
        "read_exact_by_hash_readonly",
        "read_exact_by_natural_key_in_transaction",
        "append_in_transaction",
        "read_delivery_chain_exact_in_transaction",
        "read_delivery_chain_exact_readonly",
        "append_delivery_in_transaction",
    },
    "source_revision_postgres.py": {
        "freeze_in_transaction",
        "read_exact_in_transaction",
        "read_exact_readonly",
    },
    "observation_capture_postgres.py": {
        "lock_signal_in_transaction",
        "find_header_in_transaction",
        "read_header_exact_in_transaction",
        "read_revision_chain_exact_in_transaction",
        "read_semantic_draft_for_revision_in_transaction",
        "append_materialized_bundle_in_transaction",
        "read_observation_bundle_exact_in_transaction",
        "read_observation_bundle_exact_readonly",
    },
}

FORBIDDEN_IMPORT_PREFIXES = (
    "backend.services.selection_center",
    "backend.services.strategy_package",
    "backend.services.simulation_runtime",
    "backend.services.paper_trading",
    "backend.infra.qmt",
    "backend.services.quantevolver",
    "backend.services.rdagent",
    "backend.qlib_exporter",
    "rl_execution",
    "backend.services.advisory_phase1.release_schema_apply_postgres",
    "backend.db.pg_pool",
)


def _source(file_name: str) -> str:
    return (SERVICE_ROOT / file_name).read_text(encoding="utf-8")


def _function_nodes(
    file_name: str,
) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    tree = ast.parse(_source(file_name), filename=file_name)
    return {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_g3_production_modules_do_not_cross_shared_runtime_boundaries() -> None:
    imported = []
    for file_name in PRODUCTION_FILES:
        tree = ast.parse(_source(file_name), filename=file_name)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
    assert not {
        module
        for module in imported
        if any(module.startswith(prefix) for prefix in FORBIDDEN_IMPORT_PREFIXES)
    }


def test_g3_writer_import_and_construction_do_not_load_global_pg_pool() -> None:
    script = """
import sys
from backend.services.advisory_phase1.phase1g_transactional_writer import Phase1GTransactionalWriter
Phase1GTransactionalWriter(
    transaction_connection_factory=lambda: None,
    readonly_connection_factory=lambda: None,
)
raise SystemExit(1 if 'backend.db.pg_pool' in sys.modules else 0)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_g3_writer_has_no_runtime_ddl_env_guessing_or_hidden_retry() -> None:
    for file_name in PRODUCTION_FILES:
        normalized = _source(file_name).lower()
        for forbidden in (
            "create table",
            "alter table",
            "drop table",
            "dotenv",
            "os.environ",
            "getenv(",
            "sleep(",
            "backoff",
            "select *",
            "rbac",
            "manual bypass",
            "backup gate",
        ):
            assert forbidden not in normalized


def test_caller_owned_primitives_do_not_own_connections_or_transactions() -> None:
    forbidden_calls = {"commit", "rollback", "close", "connect", "get_conn"}
    for file_name, expected_names in PRIMITIVES.items():
        nodes = _function_nodes(file_name)
        assert expected_names <= nodes.keys()
        for name in expected_names:
            calls = {
                node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
                for node in ast.walk(nodes[name])
                if isinstance(node, ast.Call)
                and isinstance(node.func, (ast.Attribute, ast.Name))
            }
            assert not calls.intersection(forbidden_calls), (file_name, name, calls)
