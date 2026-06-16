from __future__ import annotations

import re
import subprocess
from pathlib import Path

import yaml

from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.module_registry import ModuleRegistry
from backend.services.validation.plan_catalog import ALLOWED_COMMAND_KEYS, ValidationPlanCatalog


REPO_ROOT = Path(__file__).resolve().parents[3]
BLUEPRINT = REPO_ROOT / "docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md"
BASELINE_REPORT = REPO_ROOT / "docs/process/research_assistant_baseline_verification_20260531.md"
SERVICE = REPO_ROOT / "backend/services/research_assistant/service.py"
MODELS = REPO_ROOT / "backend/services/research_assistant/models.py"
SCHEMA = REPO_ROOT / "backend/db/init_research_assistant_schema_20260521.py"
QE_SERVICE = REPO_ROOT / "backend/services/quantevolver/qe_evolution_service.py"
RA_MIGRATIONS = REPO_ROOT / "backend/db/migrations/ra_upgrade"

REQUIRED_PHASE0_MODULES = {
    "research_assistant.memory_tree",
    "research_assistant.memory_curator",
    "research_assistant.graph_context",
    "research_assistant.react_grounding",
    "research_assistant.evidence_guard",
    "research_assistant.external_research",
    "research_assistant.agent_teams",
    "research_assistant.qe_autonomy",
    "research_assistant.code_intelligence",
    "research_assistant.proactive_reports",
    "research_assistant.reflection_card",
    "research_assistant.prompt_lab",
    "research_assistant.skill_library",
    "research_assistant.product_core",
    "research_assistant.core_adapter",
    "research_assistant.generic_mcp_client",
    "research_assistant.aistock_domain_adapter",
    "research_assistant.aistock_knowledge_pack",
}

OWNERSHIP_SAMPLES = {
    "backend/services/research_assistant/memory_tree.py": "research_assistant.memory_tree",
    "backend/services/research_assistant/memory_curator.py": "research_assistant.memory_curator",
    "backend/services/research_assistant/graph_context.py": "research_assistant.graph_context",
    "backend/services/research_assistant/react_grounding.py": "research_assistant.react_grounding",
    "backend/services/research_assistant/evidence_guard.py": "research_assistant.evidence_guard",
    "backend/mcp/modules/external_research.py": "research_assistant.external_research",
    "backend/services/research_assistant/agent_teams/orchestrator.py": "research_assistant.agent_teams",
    "backend/services/research_assistant/qe_autonomy.py": "research_assistant.qe_autonomy",
    "backend/services/research_assistant/code_intelligence.py": "research_assistant.code_intelligence",
    "backend/services/research_assistant/proactive_reports.py": "research_assistant.proactive_reports",
    "backend/services/research_assistant/reflection_card.py": "research_assistant.reflection_card",
    "backend/services/research_assistant/prompt_lab.py": "research_assistant.prompt_lab",
    "backend/services/research_assistant/skill_library.py": "research_assistant.skill_library",
    "backend/services/research_assistant/product_core/engine.py": "research_assistant.product_core",
    "backend/services/research_assistant/core_adapter/provider.py": "research_assistant.core_adapter",
    "backend/services/research_assistant/generic_mcp_client.py": "research_assistant.generic_mcp_client",
    "backend/services/research_assistant/aistock_domain_adapter/tools.py": "research_assistant.aistock_domain_adapter",
    "backend/services/research_assistant/aistock_knowledge_pack/loader.py": "research_assistant.aistock_knowledge_pack",
    "backend/db/migrations/ra_upgrade/001_memory_tree.sql": "research_assistant.memory_tree",
    "docs/process/research_assistant_baseline_verification_20260531.md": "research_assistant",
    "backend/tests/research_assistant/test_phase0_blueprint_baseline.py": "research_assistant",
}


def _line_number(path: Path, needle: str) -> int:
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if needle in line:
            return index
    raise AssertionError(f"{needle!r} not found in {path}")


def _table_segment(path: Path, start: str, end: str) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    start_line = _line_number(path, start) - 1
    end_line = _line_number(path, end) - 1
    return "\n".join(lines[start_line:end_line])


def test_phase0_plan_is_runner_enabled_and_allowlisted() -> None:
    plan = ValidationPlanCatalog().get_plan("ra_phase0_baseline")
    assert plan is not None
    assert plan["runner_enabled"] is True
    assert plan["command_key"] == "nox_ra_phase0_baseline"
    assert plan["nox_session"] == "ra_phase0_baseline"
    assert plan["writes_database"] is False
    assert plan["writes_business_state"] is False
    assert plan["allowed_backend_ports"] == []
    assert plan["allowed_frontend_ports"] == []
    assert ALLOWED_COMMAND_KEYS["nox_ra_phase0_baseline"] == "ra_phase0_baseline"
    nox_text = (REPO_ROOT / "noxfile.py").read_text(encoding="utf-8")
    assert "def ra_phase0_baseline" in nox_text


def test_blueprint_baseline_defects_match_current_head_lines() -> None:
    blueprint = BLUEPRINT.read_text(encoding="utf-8")
    report = BASELINE_REPORT.read_text(encoding="utf-8")

    for defect_id in [f"DEF-{index:02d}" for index in range(1, 13)]:
        assert defect_id in blueprint
        assert defect_id in report

    service_text = SERVICE.read_text(encoding="utf-8")
    service_anchors = [
        _line_number(SERVICE, "_complete_chat_with_reactive_recovery"),
        _line_number(SERVICE, "_maybe_auto_execute_read_only_mcp_route"),
        _line_number(SERVICE, "def build_context_pack"),
        _line_number(SERVICE, "def create_external_agent_session"),
        _line_number(SERVICE, "def route_model"),
        _line_number(SERVICE, "litellm.completion"),
    ]
    if "for memory_type in data.include_memory_types" in service_text:
        service_anchors.append(_line_number(SERVICE, "for memory_type in data.include_memory_types"))
    if '"graph_relation_refs": []' in service_text:
        service_anchors.append(_line_number(SERVICE, '"graph_relation_refs": []'))

    expected_anchors = {
        "service.py": service_anchors,
        "models.py": [
            _line_number(MODELS, "MEMORY_TYPES"),
        ],
        "init_research_assistant_schema_20260521.py": [
            _line_number(SCHEMA, "CREATE TABLE IF NOT EXISTS research_memory_items"),
            _line_number(SCHEMA, "CREATE TABLE IF NOT EXISTS assistant_prompt_nodes"),
            _line_number(SCHEMA, "CREATE TABLE IF NOT EXISTS assistant_external_agent_sessions"),
        ],
        "qe_evolution_service.py": [
            _line_number(QE_SERVICE, "class AutoEvolutionScheduler"),
            _line_number(QE_SERVICE, "def submit_next_loop"),
            _line_number(QE_SERVICE, "def append_custom_evo_loops"),
        ],
    }
    phase1_memory_tree_active = (RA_MIGRATIONS / "001_memory_tree.sql").exists()
    for filename, line_numbers in expected_anchors.items():
        for line_number in line_numbers:
            if phase1_memory_tree_active and filename in {"service.py", "models.py", "init_research_assistant_schema_20260521.py"}:
                assert line_number > 0
            else:
                assert f"{filename}:{line_number}" in report

    memory_table = _table_segment(
        SCHEMA,
        "CREATE TABLE IF NOT EXISTS research_memory_items",
        "CREATE INDEX IF NOT EXISTS idx_rmi_scope_type",
    )
    if phase1_memory_tree_active:
        assert "parent_key" in memory_table
        assert "tree_path" in memory_table
        assert "select_memory_branches" in service_text
    else:
        assert "parent_key" not in memory_table
        assert "tree_path" not in memory_table
        assert '"graph_relation_refs": []' in service_text
    assert not re.search(r"arxiv|scholar|tavily|web_search|paper_search", service_text, re.I)
    assert "generate_reflection_card" in service_text
    assert not re.search(r"prompt_lab|research_curriculum", service_text, re.I)


def test_phase0_module_registry_registers_all_blueprint_modules_with_owner() -> None:
    registry = ModuleRegistry()
    module_ids = registry.module_ids()
    assert REQUIRED_PHASE0_MODULES <= module_ids

    raw = yaml.safe_load((REPO_ROOT / "tests/aistock_validation/catalog/module_registry.yaml").read_text(encoding="utf-8"))
    by_id = {item["module_id"]: item for item in raw["modules"]}
    for module_id in REQUIRED_PHASE0_MODULES:
        item = by_id[module_id]
        assert item.get("parent_module") == "research_assistant"
        assert item.get("owner") == "claude_code_boundary"
        assert "ra_phase0_baseline" in (item.get("test_plans") or {}).get("required_on_change", [])


def test_phase0_file_ownership_maps_blueprint_surfaces_without_ambiguity() -> None:
    catalog = FileOwnershipCatalog(module_registry=ModuleRegistry())
    for path, expected_module in OWNERSHIP_SAMPLES.items():
        match = catalog.match_path(path)
        assert match.ownership_status == "mapped", path
        assert match.primary_module == expected_module, path
        assert match.reason_codes == (), path


def test_phase0_migration_scaffold_and_phase1_namespace_contract() -> None:
    readme = RA_MIGRATIONS / "README.md"
    assert readme.exists()
    text = readme.read_text(encoding="utf-8")
    assert "Phase 0" in text
    assert "does not define or execute DDL" in text
    assert "production_ddl_gate" in text
    phase1_sql = RA_MIGRATIONS / "001_memory_tree.sql"
    if phase1_sql.exists():
        sql = phase1_sql.read_text(encoding="utf-8")
        assert "ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS tree_path" in sql
        assert "COMMENT ON COLUMN research_memory_items.tree_path" in sql
    else:
        assert not list(RA_MIGRATIONS.glob("*.sql"))
    forbidden_sql = re.compile(r"\b(CREATE|ALTER|DROP|TRUNCATE|INSERT|UPDATE|DELETE)\b", re.I)
    assert not forbidden_sql.search(text.replace("COMMENT ON", "COMMENT_ON"))


def test_phase0_baseline_report_records_closure_and_commits() -> None:
    report = BASELINE_REPORT.read_text(encoding="utf-8")
    assert "RA-P0-01" in report
    assert "RA-P0-02" in report
    assert "RA-P0-03" in report
    assert "production_ddl_gate=noop" in report
    assert "production_backend_dependency_gate=noop" in report
    assert "production_frontend_dependency_gate=noop" in report
    assert re.search(r"baseline_source_commit: `[0-9a-f]{7,40}`", report)
    assert "DESIGN-COMPLIANCE-001" in report


def test_git_diff_check_passes_for_phase0_files() -> None:
    result = subprocess.run(
        ["git", "diff", "--check"],
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    assert result.returncode == 0, result.stdout
