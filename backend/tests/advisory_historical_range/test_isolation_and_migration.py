from __future__ import annotations

import ast
import re
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SERVICE_ROOT = REPOSITORY_ROOT / "backend" / "services" / "advisory_historical_range"
MIGRATION = REPOSITORY_ROOT / "backend" / "db" / "migrations" / "add_advisory_historical_range_phase1r_20260719.sql"


def test_phase1r_foundation_has_no_shared_runtime_imports() -> None:
    forbidden_prefixes = (
        "backend.services.simulation_runtime",
        "backend.services.selection_center",
        "backend.services.paper_trading",
        "backend.services.quantevolver",
        "backend.services.advisory_phase0a",
        "backend.qlib_exporter",
        "backend.infra.qmt",
    )
    for path in SERVICE_ROOT.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(imported.startswith(prefix) for imported in imports for prefix in forbidden_prefixes), (
            f"forbidden shared runtime import in {path.name}: {imports}"
        )


def test_migration_is_additive_isolated_and_contains_all_phase1r_entities() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    expected_tables = {
        "advisory_historical_range_batch",
        "advisory_historical_range_request_key",
        "advisory_historical_range_run",
        "advisory_historical_range_day_run",
        "advisory_historical_range_day_attempt",
        "advisory_historical_range_operation",
        "advisory_historical_range_operation_attempt",
        "advisory_historical_range_candidate",
        "advisory_historical_range_list_version",
        "advisory_historical_range_list_item",
        "advisory_historical_range_episode_snapshot",
        "advisory_historical_range_outcome",
        "advisory_historical_range_summary",
    }
    created = set(
        re.findall(
            r"CREATE TABLE IF NOT EXISTS app\.([a-z0-9_]+)",
            sql,
            flags=re.IGNORECASE,
        )
    )
    assert expected_tables <= created
    assert "ON DELETE CASCADE" not in sql.upper()
    assert "CREATE ROLE" not in sql.upper()
    assert " GRANT " not in f" {sql.upper()} "
    assert " REVOKE " not in f" {sql.upper()} "
    assert "TRUNCATE " not in sql.upper()
    assert not re.search(r"\bDROP\s+TABLE\b", sql, flags=re.IGNORECASE)
    assert "COMMENT ON COLUMN app.%I.%I" in sql

    mutation_targets = re.findall(
        r"\b(?:INSERT\s+INTO|UPDATE|ALTER\s+TABLE|DELETE\s+FROM)\s+([a-z_]+\.[a-z0-9_]+)",
        sql,
        flags=re.IGNORECASE,
    )
    assert all(target.lower().startswith("app.advisory_historical_range_") for target in mutation_targets)


def test_migration_has_runtime_invariants_without_approval_events() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()
    required_fragments = (
        "new.row_version <> old.row_version + 1",
        "day_plan_cursor_ordinal < old.day_plan_cursor_ordinal",
        "advisory_historical_range_fact_immutable",
        "advisory_historical_range_list_counts_invalid",
        "advisory_historical_range_day_chain_invalid",
        "advisory_historical_range_episode_revival_forbidden",
        "advisory_historical_range_day_canonical_input_premature",
        "advisory_historical_range_day_parent_state_invalid",
        "advisory_historical_range_day_takeover_invalid",
        "advisory_historical_range_operation_takeover_invalid",
        "advisory_historical_range_batch_child_aggregate_invalid",
        "advisory_historical_range_run_child_aggregate_invalid",
        "advisory_historical_range_day_attempt_closure_invalid",
        "advisory_historical_range_operation_attempt_closure_invalid",
        "advisory_historical_range_list_candidate_projection_invalid",
        "where status = 'running'",
        "deferrable initially deferred",
    )
    for fragment in required_fragments:
        assert fragment in sql
    assert "approval_event" not in sql
    assert "authorization_event" not in sql
    assert "package_health" not in sql
    assert "asset_validator" not in sql
