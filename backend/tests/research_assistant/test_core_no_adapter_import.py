from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CORE_FILES = [
    REPO_ROOT / "backend/services/research_assistant/memory_tree.py",
    REPO_ROOT / "backend/services/research_assistant/memory_curator.py",
    REPO_ROOT / "backend/services/research_assistant/graph_context.py",
    REPO_ROOT / "backend/services/research_assistant/code_intelligence_core.py",
    REPO_ROOT / "backend/services/research_assistant/react_grounding.py",
    REPO_ROOT / "backend/services/research_assistant/external_research.py",
    REPO_ROOT / "backend/services/research_assistant/agent_teams/models.py",
    REPO_ROOT / "backend/services/research_assistant/agent_teams/config.py",
    REPO_ROOT / "backend/services/research_assistant/agent_teams/providers.py",
    REPO_ROOT / "backend/services/research_assistant/agent_teams/runtime.py",
    REPO_ROOT / "backend/services/research_assistant/qe_autonomy/models.py",
    REPO_ROOT / "backend/services/research_assistant/qe_autonomy/providers.py",
    REPO_ROOT / "backend/services/research_assistant/qe_autonomy/guards.py",
    REPO_ROOT / "backend/services/research_assistant/qe_autonomy/runtime.py",
]
FORBIDDEN_IMPORT_PREFIXES = (
    "backend.db",
    "backend.routers",
    "backend.mcp",
    "backend.services.quantevolver",
    "backend.services.strategy",
    "backend.services.tushare",
    "backend.services.research_assistant.service",
    "backend.services.research_assistant.repository",
    "psycopg2",
)
FORBIDDEN_TOKENS = ("embed" + "ding", "vec" + "tor", "semantic_" + "search")


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def test_memory_core_modules_do_not_import_aistock_adapters_or_domain_services() -> None:
    for path in CORE_FILES:
        imported = _imports(path)
        for module_name in imported:
            assert not module_name.startswith(FORBIDDEN_IMPORT_PREFIXES), f"{path} imports adapter/domain module {module_name}"


def test_memory_core_modules_do_not_use_forbidden_similarity_retrieval() -> None:
    for path in CORE_FILES:
        text = path.read_text(encoding="utf-8").lower()
        for token in FORBIDDEN_TOKENS:
            assert token not in text, f"{path} contains forbidden retrieval token {token}"
