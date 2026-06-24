from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


ACCEPTANCE_COLUMNS = {
    "design_item",
    "implementation_refs",
    "test_or_evidence",
    "status",
    "gap_or_exception",
}

EMPTY_MARKERS = {"", "-", "n/a", "na", "none", "null", "无", "不适用"}
APPROVED_MARKERS = {
    "approved_by_user",
    "approved deviation",
    "approved_deviation",
    "user approved",
    "explicitly approved",
    "用户批准",
    "用户明确批准",
    "已批准偏差",
}
BAD_STATUS_MARKERS = {
    "todo",
    "pending",
    "partial",
    "gap",
    "blocked",
    "missing",
    "future",
    "later",
    "tbd",
    "待办",
    "待补",
    "未完成",
    "缺口",
    "阻塞",
    "后续",
}
PASS_STATUS_MARKERS = {
    "done",
    "verified",
    "pass",
    "passed",
    "complete",
    "completed",
    "ready",
    "n/a",
    "na",
    "not_applicable",
    "已完成",
    "已验证",
    "通过",
}

REQUIRED_SECTION_GROUPS = {
    "F0": [
        ("feature_card", ("feature card", "功能卡")),
        ("scope", ("scope", "范围")),
        ("design_acceptance_index", ("design acceptance index", "设计验收索引")),
        ("verification", ("verification", "验证")),
        ("production_gates", ("production gates", "生产门禁")),
    ],
    "F1": [
        ("background", ("background", "背景")),
        ("scope", ("scope", "范围")),
        ("non_goals", ("non-goals", "non goals", "边界", "非目标")),
        ("design_acceptance_index", ("design acceptance index", "设计验收索引")),
        ("implementation_plan", ("implementation plan", "实施方案")),
        ("verification_plan", ("verification plan", "验证方案")),
        ("design_acceptance_matrix", ("design acceptance matrix", "设计验收矩阵")),
        ("risks", ("risks", "failure modes", "风险", "失败模式")),
        ("production_gates", ("production gates", "生产门禁")),
    ],
    "F2": [
        ("background", ("background", "背景")),
        ("scope", ("scope", "范围")),
        ("non_goals", ("non-goals", "non goals", "边界", "非目标")),
        ("architecture", ("architecture", "架构")),
        ("contracts", ("contracts", "api/db/ui/mcp", "契约", "接口")),
        ("design_acceptance_index", ("design acceptance index", "设计验收索引")),
        ("implementation_plan", ("implementation plan", "实施方案")),
        ("verification_plan", ("verification plan", "验证方案")),
        ("design_acceptance_matrix", ("design acceptance matrix", "设计验收矩阵")),
        ("rollout_rollback", ("rollout", "rollback", "回滚", "发布")),
        ("risks", ("risks", "failure modes", "风险", "失败模式")),
        ("production_gates", ("production gates", "生产门禁")),
    ],
}


class FeatureWorkflowError(RuntimeError):
    """Raised when a feature workflow artifact violates required controls."""


@dataclass(frozen=True)
class ValidationFinding:
    code: str
    message: str
    path: str


@dataclass
class FeatureValidationResult:
    tier: str
    design_path: Path
    acceptance_path: Path
    design_items: list[str] = field(default_factory=list)
    matrix_rows: list[dict[str, str]] = field(default_factory=list)
    findings: list[ValidationFinding] = field(default_factory=list)
    warnings: list[ValidationFinding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.findings

    def compact_summary(self) -> str:
        state = "PASS" if self.ok else "FAIL"
        return (
            f"Feature workflow validation: {state}\n"
            f"tier={self.tier} design_items={len(self.design_items)} "
            f"matrix_rows={len(self.matrix_rows)} warnings={len(self.warnings)}"
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "tier": self.tier,
            "design_path": str(self.design_path),
            "acceptance_path": str(self.acceptance_path),
            "design_items": self.design_items,
            "matrix_row_count": len(self.matrix_rows),
            "findings": [finding.__dict__ for finding in self.findings],
            "warnings": [warning.__dict__ for warning in self.warnings],
        }


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _normalize_cell(value: str) -> str:
    return value.strip().strip("`").strip()


def _header_lines(markdown: str) -> list[str]:
    return [
        line.lstrip("#").strip()
        for line in markdown.splitlines()
        if line.lstrip().startswith("#")
    ]


def _has_section(headers: Iterable[str], aliases: tuple[str, ...]) -> bool:
    normalized_headers = [_normalize_text(header) for header in headers]
    for header in normalized_headers:
        if any(alias.lower() in header for alias in aliases):
            return True
    return False


def _extract_design_items(markdown: str) -> list[str]:
    return sorted(set(re.findall(r"\bF-\d{3}\b", markdown)))


def _split_table_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return []
    return [_normalize_cell(cell) for cell in stripped.strip("|").split("|")]


def _is_separator_row(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in cells)


def _parse_acceptance_matrix(markdown: str) -> list[dict[str, str]]:
    lines = markdown.splitlines()
    rows: list[dict[str, str]] = []
    for index in range(len(lines) - 1):
        header_cells = _split_table_row(lines[index])
        separator_cells = _split_table_row(lines[index + 1])
        if not header_cells or not _is_separator_row(separator_cells):
            continue
        normalized_headers = [_normalize_text(cell).replace(" ", "_") for cell in header_cells]
        if not ACCEPTANCE_COLUMNS.issubset(set(normalized_headers)):
            continue
        header_to_index = {header: idx for idx, header in enumerate(normalized_headers)}
        row_index = index + 2
        while row_index < len(lines):
            cells = _split_table_row(lines[row_index])
            if not cells:
                break
            row = {
                column: cells[header_to_index[column]] if header_to_index[column] < len(cells) else ""
                for column in ACCEPTANCE_COLUMNS
            }
            rows.append(row)
            row_index += 1
    return rows


def _contains_approved_marker(value: str) -> bool:
    normalized = _normalize_text(value)
    return any(marker in normalized for marker in APPROVED_MARKERS)


def _is_empty_marker(value: str) -> bool:
    return _normalize_text(value) in EMPTY_MARKERS


def _is_approved_status(status: str, gap: str) -> bool:
    return _contains_approved_marker(status) or _contains_approved_marker(gap)


def _line_is_guardrail_statement(line: str) -> bool:
    normalized = _normalize_text(line)
    return any(
        marker in normalized
        for marker in ("禁止", "不得", "严禁", "not ", "never", "must not", "no simplified", "no mock")
    ) or any(
        marker in normalized
        for marker in ("reject", "rejects", "failure", "失败", "拒绝")
    )


def _find_simplified_completion_lines(markdown: str) -> list[str]:
    risky_terms = r"(简化版|子集版|占位版|poc|proof-of-concept|mock-only|static success|静态成功|placeholder)"
    completion_terms = r"(完成|交付|可合入|ready|done|complete|deliver|verified|pass)"
    pattern_a = re.compile(rf"{risky_terms}.{{0,50}}{completion_terms}", re.IGNORECASE)
    pattern_b = re.compile(rf"{completion_terms}.{{0,50}}{risky_terms}", re.IGNORECASE)
    hits: list[str] = []
    seen: set[str] = set()
    for line in markdown.splitlines():
        if _line_is_guardrail_statement(line):
            continue
        stripped = line.strip()
        if (pattern_a.search(line) or pattern_b.search(line)) and stripped not in seen:
            seen.add(stripped)
            hits.append(stripped)
    return hits


def validate_feature_artifacts(
    *,
    design_path: Path,
    acceptance_path: Path | None = None,
    tier: str,
) -> FeatureValidationResult:
    normalized_tier = tier.upper()
    if normalized_tier not in REQUIRED_SECTION_GROUPS:
        raise FeatureWorkflowError(f"Unsupported feature tier: {tier}")
    acceptance_path = acceptance_path or design_path
    design = design_path.read_text(encoding="utf-8")
    acceptance = acceptance_path.read_text(encoding="utf-8")
    result = FeatureValidationResult(
        tier=normalized_tier,
        design_path=design_path,
        acceptance_path=acceptance_path,
    )

    headers = _header_lines(design)
    for section_code, aliases in REQUIRED_SECTION_GROUPS[normalized_tier]:
        if not _has_section(headers, aliases):
            result.findings.append(
                ValidationFinding(
                    code="missing_required_section",
                    message=f"{normalized_tier} design is missing required section group: {section_code}",
                    path=str(design_path),
                )
            )

    result.design_items = _extract_design_items(design)
    if not result.design_items:
        result.findings.append(
            ValidationFinding(
                code="missing_design_acceptance_index",
                message="Design Acceptance Index must contain stable item ids like F-001.",
                path=str(design_path),
            )
        )

    for line in _find_simplified_completion_lines(design + "\n" + acceptance):
        result.findings.append(
            ValidationFinding(
                code="simplified_completion_language",
                message=f"Simplified, POC, placeholder, static, or mock-only delivery is described as complete: {line[:180]}",
                path=str(design_path),
            )
        )

    result.matrix_rows = _parse_acceptance_matrix(acceptance)
    if not result.matrix_rows:
        result.findings.append(
            ValidationFinding(
                code="missing_acceptance_matrix",
                message="Acceptance artifact must include a markdown table with design_item, implementation_refs, test_or_evidence, status, gap_or_exception.",
                path=str(acceptance_path),
            )
        )
        return result

    covered_items = {row.get("design_item", "").strip() for row in result.matrix_rows}
    for item in result.design_items:
        if item not in covered_items:
            result.findings.append(
                ValidationFinding(
                    code="acceptance_item_not_covered",
                    message=f"Design item {item} is not covered by the acceptance matrix.",
                    path=str(acceptance_path),
                )
            )

    for row_number, row in enumerate(result.matrix_rows, start=1):
        item = row.get("design_item", "").strip() or f"row-{row_number}"
        for column in ("implementation_refs", "test_or_evidence", "status"):
            if _is_empty_marker(row.get(column, "")):
                result.findings.append(
                    ValidationFinding(
                        code="acceptance_required_cell_empty",
                        message=f"{item} has empty required cell: {column}",
                        path=str(acceptance_path),
                    )
                )
        status = row.get("status", "")
        gap = row.get("gap_or_exception", "")
        normalized_status = _normalize_text(status)
        if any(marker in normalized_status for marker in BAD_STATUS_MARKERS) and not _is_approved_status(status, gap):
            result.findings.append(
                ValidationFinding(
                    code="unapproved_incomplete_status",
                    message=f"{item} has unapproved incomplete status: {status}",
                    path=str(acceptance_path),
                )
            )
        if not _is_empty_marker(gap) and not _is_approved_status(status, gap):
            result.findings.append(
                ValidationFinding(
                    code="unapproved_gap_or_exception",
                    message=f"{item} has a gap_or_exception without explicit user approval.",
                    path=str(acceptance_path),
                )
            )
        if (
            not any(marker in normalized_status for marker in PASS_STATUS_MARKERS)
            and not _is_approved_status(status, gap)
        ):
            result.findings.append(
                ValidationFinding(
                    code="unknown_acceptance_status",
                    message=f"{item} uses unsupported acceptance status: {status}",
                    path=str(acceptance_path),
                )
            )
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate AIstock feature design and design-acceptance artifacts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "pr-summary"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--design", required=True, type=Path)
        subparser.add_argument("--acceptance", type=Path)
        subparser.add_argument("--tier", choices=("F0", "F1", "F2", "f0", "f1", "f2"), required=True)
        subparser.add_argument("--format", choices=("summary", "json"), default="summary")
    return parser


def _print_result(result: FeatureValidationResult, *, output_format: str, command: str) -> None:
    if output_format == "json":
        print(json.dumps(result.to_payload(), ensure_ascii=False, indent=2))
        return
    print(result.compact_summary())
    if command == "pr-summary":
        print(
            "PR checklist: design_doc=present acceptance_matrix=checked "
            f"design_items={len(result.design_items)} production_gates=declared"
        )
    if not result.ok:
        for finding in result.findings[:20]:
            print(f"- {finding.code}: {finding.message}")
        if len(result.findings) > 20:
            print(f"- ... {len(result.findings) - 20} additional finding(s) omitted")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = validate_feature_artifacts(
            design_path=args.design,
            acceptance_path=args.acceptance,
            tier=args.tier,
        )
    except FeatureWorkflowError as exc:
        print(f"Feature workflow validation: FAIL\n- configuration_error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"Feature workflow validation: FAIL\n- file_error: {exc}", file=sys.stderr)
        return 2
    _print_result(result, output_format=args.format, command=args.command)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
