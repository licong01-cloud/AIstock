from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


VALID_STATUSES = {"hard_pass", "future_phase_pending", "approved_exception"}
REQUIRED_DEFECTS = {f"DEF-{index:02d}" for index in range(1, 14)}
HARD_PASS_DAI_PREFIXES = (
    "DAI-MEM-",
    "DAI-GND-",
    "DAI-GRAPH-",
    "DAI-EXT-",
    "DAI-TEAM-",
    "DAI-QE-",
    "DAI-PARADIGM-",
)
HARD_PASS_DAI_IDS = {"DAI-DRIFT-001"}
PHASE8_HARD_PASS_DAI_IDS = {"DAI-CODE-001", "DAI-CODE-002"}
PHASE9_HARD_PASS_DAI_IDS = {"DAI-REPORT-001"}
PHASE10_HARD_PASS_DAI_IDS = {"DAI-LEARN-001"}
PHASE11_HARD_PASS_DAI_IDS = {"DAI-LEARN-002"}
PHASE12_HARD_PASS_DAI_IDS = {"DAI-LEARN-002"}


@dataclass(frozen=True)
class TraceabilityRow:
    section: str
    design_item: str
    defect: str
    implementation: str
    test: str


@dataclass(frozen=True)
class DaiRow:
    section: str
    dai_id: str
    requirement: str
    design_ref: str
    acceptance: str


def _section_lines(lines: list[str], heading: str) -> list[str]:
    start: int | None = None
    heading_level = len(heading) - len(heading.lstrip("#"))
    for index, line in enumerate(lines):
        if line.startswith(heading):
            start = index + 1
            break
    if start is None:
        raise AssertionError(f"heading not found: {heading}")
    end = len(lines)
    for index in range(start, len(lines)):
        line = lines[index]
        if line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            if level <= heading_level:
                end = index
                break
    return lines[start:end]


def _first_markdown_table(section: list[str]) -> list[list[str]]:
    table_lines: list[str] = []
    in_table = False
    for line in section:
        if line.strip().startswith("|") and line.strip().endswith("|"):
            table_lines.append(line.strip())
            in_table = True
            continue
        if in_table and table_lines:
            break
    if len(table_lines) < 3:
        raise AssertionError("markdown table not found")
    rows: list[list[str]] = []
    for line in table_lines[2:]:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells:
            rows.append(cells)
    return rows


def parse_traceability_matrix(path: Path) -> list[TraceabilityRow]:
    lines = path.read_text(encoding="utf-8").splitlines()
    rows: list[TraceabilityRow] = []
    for heading in ("## 12. 可追溯性矩阵", "### 16.9 可追溯性矩阵", "### 17.10 可追溯性矩阵"):
        for cells in _first_markdown_table(_section_lines(lines, heading)):
            if len(cells) < 4:
                raise AssertionError(f"traceability row has too few cells in {heading}: {cells}")
            rows.append(TraceabilityRow(heading, cells[0], cells[1], cells[2], cells[3]))
    return rows


def parse_dai_sections(path: Path) -> list[DaiRow]:
    lines = path.read_text(encoding="utf-8").splitlines()
    rows: list[DaiRow] = []
    for heading in ("## 13. Design Acceptance Index", "### 16.10 Design Acceptance Index", "### 17.11 Design Acceptance Index"):
        for cells in _first_markdown_table(_section_lines(lines, heading)):
            if len(cells) < 4:
                raise AssertionError(f"DAI row has too few cells in {heading}: {cells}")
            rows.append(DaiRow(heading, cells[0], cells[1], cells[2], cells[3]))
    return rows


def _load_expected(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise AssertionError("expected manifest must be a mapping")
    return data


def _validate_status_map(name: str, values: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for key, raw in values.items():
        if not isinstance(raw, Mapping):
            raise AssertionError(f"{name}.{key} must be a mapping")
        status = str(raw.get("status") or "")
        if status not in VALID_STATUSES:
            raise AssertionError(f"{name}.{key} has invalid status {status!r}")
        normalized[str(key)] = dict(raw)
    return normalized


def _assert_traceability_rows(rows: list[TraceabilityRow]) -> None:
    for row in rows:
        for field_name, value in (
            ("design_item", row.design_item),
            ("defect", row.defect),
            ("implementation", row.implementation),
            ("test", row.test),
        ):
            if not value.strip():
                raise AssertionError(f"empty {field_name} in {row.section}: {row.design_item}")


def assert_no_defect_omissions(blueprint: str, defect_map: Mapping[str, Any] | None = None) -> None:
    missing_in_doc = sorted(defect for defect in REQUIRED_DEFECTS if defect not in blueprint)
    if missing_in_doc:
        raise AssertionError(f"blueprint missing defect ids: {missing_in_doc}")
    if defect_map is not None:
        missing_in_manifest = sorted(REQUIRED_DEFECTS - set(defect_map))
        if missing_in_manifest:
            raise AssertionError(f"expected manifest missing defect classifications: {missing_in_manifest}")


def _assert_dai_classification(rows: list[DaiRow], expected: Mapping[str, Any]) -> None:
    dai_map = _validate_status_map("dai_classification", expected)
    doc_ids = {row.dai_id for row in rows}
    missing = sorted(doc_ids - set(dai_map))
    extra = sorted(set(dai_map) - doc_ids)
    if missing:
        raise AssertionError(f"expected manifest missing DAI classifications: {missing}")
    if extra:
        raise AssertionError(f"expected manifest has unknown DAI ids: {extra}")
    for row in rows:
        status = str(dai_map[row.dai_id]["status"])
        if row.section.startswith("## 13.") and status != "hard_pass":
            raise AssertionError(f"original §13 DAI must be hard_pass in Phase 7: {row.dai_id} -> {status}")
        if (row.dai_id.startswith(HARD_PASS_DAI_PREFIXES) or row.dai_id in HARD_PASS_DAI_IDS) and status != "hard_pass":
            raise AssertionError(f"Phase 7 hard DAI cannot be downgraded: {row.dai_id} -> {status}")
        if row.dai_id in PHASE8_HARD_PASS_DAI_IDS and status == "future_phase_pending":
            raise AssertionError(f"Phase 8 code intelligence DAI cannot remain pending: {row.dai_id}")
        if row.dai_id in PHASE9_HARD_PASS_DAI_IDS and status == "future_phase_pending":
            raise AssertionError(f"Phase 9 proactive report DAI cannot remain pending: {row.dai_id}")
        if row.dai_id in PHASE10_HARD_PASS_DAI_IDS and status == "future_phase_pending":
            raise AssertionError(f"Phase 10 reflection DAI cannot remain pending: {row.dai_id}")
        if row.dai_id in PHASE11_HARD_PASS_DAI_IDS and status == "future_phase_pending":
            raise AssertionError(f"Phase 11 prompt lab DAI cannot remain pending: {row.dai_id}")
        if row.dai_id in PHASE12_HARD_PASS_DAI_IDS and status == "future_phase_pending":
            raise AssertionError(f"Phase 12 skill library DAI cannot remain pending: {row.dai_id}")


def assert_phase0_6_anchors(expected: Mapping[str, Any], actual_text: str) -> None:
    for phase, raw in expected.items():
        if not isinstance(raw, Mapping):
            raise AssertionError(f"phase anchor {phase} must be a mapping")
        for token in raw.get("required_tokens") or []:
            if str(token) not in actual_text:
                raise AssertionError(f"phase anchor drift: {phase} missing token {token!r}")


def run_crosscheck(blueprint: Path, expected: Path, *, fail_on_drift: bool) -> dict[str, Any]:
    blueprint_text = blueprint.read_text(encoding="utf-8")
    traceability_rows = parse_traceability_matrix(blueprint)
    dai_rows = parse_dai_sections(blueprint)
    manifest = _load_expected(expected)
    defect_map = _validate_status_map("defect_classification", manifest.get("defect_classification") or {})

    _assert_traceability_rows(traceability_rows)
    assert_no_defect_omissions(blueprint_text, defect_map)
    _assert_dai_classification(dai_rows, manifest.get("dai_classification") or {})
    if fail_on_drift:
        assert_phase0_6_anchors(manifest.get("phase0_6_anchors") or {}, blueprint_text)

    return {
        "status": "passed",
        "traceability_rows": len(traceability_rows),
        "dai_rows": len(dai_rows),
        "defect_classifications": len(defect_map),
        "phase_anchor_count": len(manifest.get("phase0_6_anchors") or {}),
        "tri_state_statuses": sorted(VALID_STATUSES),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blueprint", type=Path, required=True)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--fail-on-drift", action="store_true")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()

    result = run_crosscheck(args.blueprint, args.expected, fail_on_drift=args.fail_on_drift)
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
