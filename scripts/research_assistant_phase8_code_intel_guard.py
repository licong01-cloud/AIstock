from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_FILES = [
    REPO_ROOT / "backend/services/research_assistant/code_intelligence_core.py",
]
PHASE8_SOURCE_FILES = [
    *CORE_FILES,
    REPO_ROOT / "backend/services/research_assistant/code_intelligence_adapter_provider.py",
    REPO_ROOT / "backend/services/research_assistant/code_context_refs_repository.py",
]
FORBIDDEN_CORE_IMPORTS = (
    "backend.db",
    "backend.routers",
    "backend.services.research_assistant.code_intelligence_adapter_provider",
    "backend.services.research_assistant.repository",
    "backend.services.research_assistant.service",
    "psycopg2",
    "scripts.code_intelligence_adapter",
)
REQUIRED_ADAPTER_SYMBOLS = (
    "codegraph_status",
    "build_context_artifacts",
    "build_affected_tests_artifact",
    "build_understand_anything_summary",
    "build_understand_anything_summary_manifest",
)


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def _failures(args: argparse.Namespace) -> list[str]:
    failures: list[str] = []
    if args.fail_on_core_adapter_import:
        for path in CORE_FILES:
            for module in _imports(path):
                if module.startswith(FORBIDDEN_CORE_IMPORTS):
                    failures.append(f"{path.relative_to(REPO_ROOT)} imports forbidden adapter/domain module {module}")
    if args.fail_on_embedding:
        blocked = ("embed" + "ding", "vec" + "tor", "similar" + "ity")
        for path in CORE_FILES:
            text = path.read_text(encoding="utf-8").lower()
            for token in blocked:
                if token in text:
                    failures.append(f"{path.relative_to(REPO_ROOT)} contains forbidden token {token}")
    if args.fail_on_nondeterminism:
        for path in PHASE8_SOURCE_FILES:
            text = path.read_text(encoding="utf-8")
            for token in ("datetime.now", "date.today"):
                if token in text:
                    failures.append(f"{path.relative_to(REPO_ROOT)} fabricates as_of with {token}")
            if "except Exception:" in text or "except Exception as" in text:
                failures.append(f"{path.relative_to(REPO_ROOT)} contains broad exception handling")
    provider = (REPO_ROOT / "backend/services/research_assistant/code_intelligence_adapter_provider.py").read_text(encoding="utf-8")
    if "import scripts.code_intelligence_adapter as adapter" not in provider:
        failures.append("adapter provider does not directly import scripts.code_intelligence_adapter")
    for symbol in REQUIRED_ADAPTER_SYMBOLS:
        if f"adapter.{symbol}" not in provider:
            failures.append(f"adapter provider does not call existing symbol {symbol}")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-on-embedding", action="store_true")
    parser.add_argument("--fail-on-nondeterminism", action="store_true")
    parser.add_argument("--fail-on-core-adapter-import", action="store_true")
    args = parser.parse_args(argv)
    failures = _failures(args)
    if failures:
        for failure in failures:
            print(f"RA Phase8 guard failure: {failure}", file=sys.stderr)
        return 1
    print("RA Phase8 code intelligence guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
