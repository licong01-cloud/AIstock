from __future__ import annotations

import argparse
import io
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "aistock_legacy_inventory_v1"
TEXT_SUFFIXES = {
    ".bat",
    ".css",
    ".env",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".ps1",
    ".py",
    ".sh",
    ".sql",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}
MAX_TEXT_BYTES = 1_500_000

PROTECTED_PREFIXES = (
    ".git/",
    "backend/db/",
    "backend/migrations/",
    "docs/standards/",
    "frontend/.next/",
    "mlruns/",
    "node_modules/",
    "qe_archive/artifacts/",
    "rdagent_assets/",
)
PROTECTED_EXACT = {
    "AGENTS.md",
    "AGENTS.override.md",
    "CLAUDE.md",
    "README.md",
    "noxfile.py",
    "pyproject.toml",
}
ROOT_STABLE_FILES = {
    ".env.example",
    ".gitignore",
    ".pre-commit-config.yaml",
    "AIstock.code-workspace",
    "requirements-dev.txt",
    "requirements.lock.txt",
    "requirements.txt",
    "start_all_ai_stock.bat",
}
SCRIPT_REVIEW_KEYWORDS = (
    "debug",
    "diagnos",
    "inspect",
    "mini",
    "one_off",
    "probe",
    "simple",
    "smoke",
    "test_",
    "verify",
)
SCRIPT_REVIEW_VERSION_MARKERS = ("v2", "v3", "v24", "v25", "v26")


@dataclass(frozen=True)
class InventoryItem:
    path: str
    category: str
    lifecycle_status: str
    risk: str
    confidence: str
    recommended_action: str
    signals: tuple[str, ...]
    references_found: int
    reference_examples: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "category": self.category,
            "lifecycle_status": self.lifecycle_status,
            "risk": self.risk,
            "confidence": self.confidence,
            "recommended_action": self.recommended_action,
            "signals": list(self.signals),
            "references_found": self.references_found,
            "reference_examples": list(self.reference_examples),
        }


def _git_output(args: list[str], root: Path) -> str:
    return subprocess.check_output(
        args,
        cwd=root,
        text=True,
        encoding="utf-8",
        errors="replace",
        stderr=subprocess.DEVNULL,
    )


def git_tracked_files(root: Path) -> list[str]:
    output = _git_output(["git", "ls-files"], root)
    return [line.strip().replace("\\", "/") for line in output.splitlines() if line.strip()]


def is_protected_path(path_key: str) -> bool:
    return path_key in PROTECTED_EXACT or any(path_key.startswith(prefix) for prefix in PROTECTED_PREFIXES)


def _is_root_path(path_key: str) -> bool:
    return "/" not in path_key


def _script_needs_lifecycle_review(path_key: str) -> bool:
    if not path_key.startswith("scripts/") or not path_key.endswith(".py"):
        return False
    lower_path = path_key.lower()
    if lower_path.startswith("scripts/diagnostics/"):
        return True
    return any(keyword in lower_path for keyword in SCRIPT_REVIEW_KEYWORDS) or any(
        marker in lower_path for marker in SCRIPT_REVIEW_VERSION_MARKERS
    )


def classify_candidate(path_key: str) -> tuple[str, tuple[str, ...]] | None:
    if is_protected_path(path_key):
        return None

    signals: list[str] = []
    if _is_root_path(path_key):
        if path_key in ROOT_STABLE_FILES:
            return None
        suffix = Path(path_key).suffix.lower()
        if suffix == ".py":
            signals.extend(["root_level_file", "python_module"])
            return "root_python_review", tuple(signals)
        if suffix == ".md":
            signals.extend(["root_level_file", "markdown_document"])
            return "root_document_review", tuple(signals)
        if suffix in {".sql", ".txt", ".json"}:
            signals.extend(["root_level_file", f"{suffix[1:]}_file"])
            return "root_misc_review", tuple(signals)

    if path_key.startswith("docs/") and path_key.count("/") == 1 and path_key.endswith(".md"):
        signals.extend(["docs_root_markdown", "needs_doc_taxonomy"])
        return "legacy_doc_review", tuple(signals)

    if _script_needs_lifecycle_review(path_key):
        signals.extend(["script_lifecycle_review"])
        return "script_lifecycle_review", tuple(signals)

    return None


def _module_tokens(path_key: str) -> tuple[str, ...]:
    path = Path(path_key)
    tokens = {path_key, path.name}
    if path.suffix == ".py":
        dotted = path_key[:-3].replace("/", ".")
        tokens.update(
            {
                dotted,
                f"import {dotted}",
                f"from {dotted}",
                f"import {path.stem}",
                f"from {path.stem}",
            }
        )
    return tuple(sorted(tokens, key=len, reverse=True))


def _read_text_if_small(path: Path) -> str:
    if not path.exists() or not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
        return ""
    if path.stat().st_size > MAX_TEXT_BYTES:
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def _build_text_index(paths: Iterable[str], root: Path) -> dict[str, str]:
    text_index: dict[str, str] = {}
    for path_key in paths:
        text = _read_text_if_small(root / path_key)
        if text:
            text_index[path_key] = text
    return text_index


def _reference_examples(path_key: str, text_index: dict[str, str], max_examples: int) -> tuple[str, ...]:
    tokens = _module_tokens(path_key)
    examples: list[str] = []
    for other_key, text in text_index.items():
        if other_key == path_key:
            continue
        if any(token in text for token in tokens):
            examples.append(other_key)
            if len(examples) >= max_examples:
                break
    return tuple(examples)


def _score_item(category: str, references_found: int) -> tuple[str, str, str, str]:
    if category == "root_python_review":
        if references_found:
            return "deprecated", "high", "low", "review_root_python_entrypoint"
        return "delete_candidate", "high", "medium", "confirm_imports_then_move_or_remove"
    if category in {"root_document_review", "legacy_doc_review"}:
        if references_found:
            return "legacy_readonly", "medium", "low", "classify_document_and_link_from_index"
        return "delete_candidate", "medium", "medium", "classify_archive_or_remove_after_review"
    if category == "root_misc_review":
        return "delete_candidate", "medium", "medium", "move_to_owned_directory_or_remove_after_review"
    if category == "script_lifecycle_review":
        if references_found:
            return "deprecated", "medium", "low", "promote_to_formal_script_or_keep_with_tests"
        return "delete_candidate", "medium", "medium", "move_to_debug_tools_or_remove_after_review"
    return "legacy_readonly", "low", "low", "manual_review"


def collect_inventory(root: Path, paths: Iterable[str], max_reference_examples: int = 5) -> list[InventoryItem]:
    normalized_paths = sorted({path.replace("\\", "/") for path in paths})
    text_index = _build_text_index(normalized_paths, root)
    items: list[InventoryItem] = []
    for path_key in normalized_paths:
        candidate = classify_candidate(path_key)
        if candidate is None:
            continue
        category, signals = candidate
        reference_examples = _reference_examples(
            path_key,
            text_index,
            max_examples=max_reference_examples,
        )
        lifecycle_status, risk, confidence, action = _score_item(category, len(reference_examples))
        if not (root / path_key).exists():
            signals = (*signals, "missing_in_worktree")
            confidence = "low"
        items.append(
            InventoryItem(
                path=path_key,
                category=category,
                lifecycle_status=lifecycle_status,
                risk=risk,
                confidence=confidence,
                recommended_action=action,
                signals=signals,
                references_found=len(reference_examples),
                reference_examples=reference_examples,
            )
        )
    return items


def summarize(items: Iterable[InventoryItem]) -> dict[str, Any]:
    by_category: dict[str, int] = {}
    by_status: dict[str, int] = {}
    by_risk: dict[str, int] = {}
    materialized = list(items)
    for item in materialized:
        by_category[item.category] = by_category.get(item.category, 0) + 1
        by_status[item.lifecycle_status] = by_status.get(item.lifecycle_status, 0) + 1
        by_risk[item.risk] = by_risk.get(item.risk, 0) + 1
    return {
        "total_items": len(materialized),
        "by_category": dict(sorted(by_category.items())),
        "by_lifecycle_status": dict(sorted(by_status.items())),
        "by_risk": dict(sorted(by_risk.items())),
    }


def write_json(path: Path, items: list[InventoryItem], files_scanned: int, mode: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "files_scanned": files_scanned,
        "summary": summarize(items),
        "items": [item.to_dict() for item in items],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_summary_md(path: Path, items: list[InventoryItem], files_scanned: int, mode: str, max_items: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = summarize(items)
    lines = [
        "# AIstock Legacy Inventory Baseline",
        "",
        f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
        f"- Mode: `{mode}`",
        f"- Files scanned: {files_scanned}",
        f"- Inventory items: {summary['total_items']}",
        "- Safety: read-only inventory; this is not a deletion list.",
        "",
        "## Summary By Category",
        "",
        "| Category | Count |",
        "|---|---:|",
    ]
    for category, count in summary["by_category"].items():
        lines.append(f"| `{category}` | {count} |")
    lines.extend(["", "## Summary By Lifecycle Status", "", "| Status | Count |", "|---|---:|"])
    for status, count in summary["by_lifecycle_status"].items():
        lines.append(f"| `{status}` | {count} |")
    lines.extend(["", "## Summary By Risk", "", "| Risk | Count |", "|---|---:|"])
    for risk, count in summary["by_risk"].items():
        lines.append(f"| `{risk}` | {count} |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This baseline identifies lifecycle-review candidates only.",
            "Do not delete or move any file without module review and targeted validation.",
            "Items with references or high-risk categories require extra review before any cleanup.",
            "",
            f"## First {min(max_items, len(items))} Items",
            "",
            "| Path | Category | Status | Risk | Confidence | References | Recommended Action |",
            "|---|---|---|---|---|---:|---|",
        ]
    )
    for item in items[:max_items]:
        lines.append(
            "| "
            f"`{item.path}` | `{item.category}` | `{item.lifecycle_status}` | `{item.risk}` | "
            f"`{item.confidence}` | {item.references_found} | `{item.recommended_action}` |"
        )
    if len(items) > max_items:
        lines.append("")
        lines.append(f"Report truncated to {max_items} items. See JSON output for full machine-readable details.")
    buffer = io.StringIO()
    for line in lines:
        buffer.write(line)
        buffer.write("\n")
    path.write_text(buffer.getvalue(), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only AIstock legacy/dead-code inventory.")
    parser.add_argument("paths", nargs="*", help="Optional paths to classify instead of all tracked files.")
    parser.add_argument("--output-json", help="Write machine-readable inventory JSON.")
    parser.add_argument("--summary-md", help="Write human-readable Markdown summary.")
    parser.add_argument("--max-items-md", type=int, default=200, help="Maximum items included in Markdown.")
    parser.add_argument("--max-reference-examples", type=int, default=5, help="Reference examples per item.")
    args = parser.parse_args()

    root = Path.cwd()
    if args.paths:
        mode = "paths"
        paths = [str(Path(path).as_posix()).replace("\\", "/") for path in args.paths]
    else:
        mode = "tracked_files"
        paths = git_tracked_files(root)

    items = collect_inventory(root=root, paths=paths, max_reference_examples=args.max_reference_examples)
    summary = summarize(items)
    print(
        "Legacy inventory completed: "
        f"mode={mode}, files={len(paths)}, items={summary['total_items']}, "
        f"by_category={summary['by_category']}"
    )

    if args.output_json:
        write_json(root / args.output_json, items=items, files_scanned=len(paths), mode=mode)
    if args.summary_md:
        write_summary_md(root / args.summary_md, items=items, files_scanned=len(paths), mode=mode, max_items=args.max_items_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
