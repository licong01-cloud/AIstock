from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / "scripts" / "aistock_feature_workflow.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("aistock_feature_workflow", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write(path: Path, content: str) -> Path:
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path


VALID_F0_CARD = """
# Feature Card

## Scope

- Add a compact workflow guard.

## Design Acceptance Index

- F-001: Validate design sections.
- F-002: Validate acceptance evidence.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/aistock_feature_workflow.py | pytest targeted test | verified | - |
| F-002 | backend/tests/scripts/test_aistock_feature_workflow.py | pytest targeted test | done | - |

## Verification

- python -m pytest backend/tests/scripts/test_aistock_feature_workflow.py -q

## Production Gates

- production_ddl_gate=noop
"""


VALID_F1_DOC = """
# Feature Workflow v1 Design

## Background

Feature delivery needs a design-based check.

## Scope

- F-001: Enforce required design sections.
- F-002: Enforce acceptance matrix coverage.

## Non-Goals

- No heavy persistent feature registry.

## Design Acceptance Index

- F-001: The CLI rejects incomplete design docs.
- F-002: The CLI rejects unapproved gaps.

## Implementation Plan

- Add a lightweight script.

## Verification Plan

- Use targeted tests and compact summary output.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/aistock_feature_workflow.py | unit test | verified | - |
| F-002 | scripts/aistock_feature_workflow.py | unit test | completed | - |

## Risks

- The guard must not become a heavy process.

## Production Gates

- production_ddl_gate=noop
"""


VALID_F2_DOC = """
# Feature Workflow F2 Design

## Background

Cross-module features need stronger review.

## Scope

- F-001: Validate architecture sections.

## Non-Goals

- No production service restart.

## Architecture

- CLI validates markdown artifacts.

## Contracts

- Inputs are design and acceptance markdown files.

## Design Acceptance Index

- F-001: F2 requires architecture and contract sections.

## Implementation Plan

- Add parser and tests.

## Verification Plan

- Run targeted tests.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/aistock_feature_workflow.py | pytest | pass | - |

## Rollout / Rollback

- Merge only after green checks.

## Risks

- Over-validation can waste time.

## Production Gates

- production_ddl_gate=noop
"""


def test_f0_lightweight_card_valid(tmp_path: Path) -> None:
    workflow = _load_module()
    design = _write(tmp_path / "feature_card.md", VALID_F0_CARD)

    result = workflow.validate_feature_artifacts(design_path=design, tier="F0")

    assert result.ok
    assert result.design_items == ["F-001", "F-002"]
    assert len(result.matrix_rows) == 2


def test_f1_missing_required_sections_fails(tmp_path: Path) -> None:
    workflow = _load_module()
    design = _write(
        tmp_path / "bad_design.md",
        """
        # Missing Design

        ## Scope
        - F-001: A feature.
        """,
    )

    result = workflow.validate_feature_artifacts(design_path=design, tier="F1")

    assert not result.ok
    assert any(finding.code == "missing_required_section" for finding in result.findings)
    assert any(finding.code == "missing_acceptance_matrix" for finding in result.findings)


def test_f2_valid_requires_architecture_and_contracts(tmp_path: Path) -> None:
    workflow = _load_module()
    design = _write(tmp_path / "f2.md", VALID_F2_DOC)

    result = workflow.validate_feature_artifacts(design_path=design, tier="F2")

    assert result.ok


def test_acceptance_matrix_with_unapproved_gap_fails(tmp_path: Path) -> None:
    workflow = _load_module()
    design = _write(
        tmp_path / "gap.md",
        VALID_F1_DOC.replace("| F-002 | scripts/aistock_feature_workflow.py | unit test | completed | - |", "| F-002 | scripts/aistock_feature_workflow.py | unit test | partial | follow later |"),
    )

    result = workflow.validate_feature_artifacts(design_path=design, tier="F1")

    assert not result.ok
    assert any(finding.code == "unapproved_incomplete_status" for finding in result.findings)
    assert any(finding.code == "unapproved_gap_or_exception" for finding in result.findings)


def test_user_approved_deviation_is_allowed(tmp_path: Path) -> None:
    workflow = _load_module()
    design = _write(
        tmp_path / "approved_gap.md",
        VALID_F1_DOC.replace("| F-002 | scripts/aistock_feature_workflow.py | unit test | completed | - |", "| F-002 | scripts/aistock_feature_workflow.py | unit test | approved_by_user | user approved deviation: deferred by scope |"),
    )

    result = workflow.validate_feature_artifacts(design_path=design, tier="F1")

    assert result.ok


def test_simplified_or_mock_only_completion_language_fails(tmp_path: Path) -> None:
    workflow = _load_module()
    design = _write(
        tmp_path / "simplified.md",
        VALID_F1_DOC + "\n## Completion Note\n\nPOC version is ready for merge.\n",
    )

    result = workflow.validate_feature_artifacts(design_path=design, tier="F1")

    assert not result.ok
    assert any(finding.code == "simplified_completion_language" for finding in result.findings)


def test_cli_compact_summary_has_no_raw_payload(tmp_path: Path, capsys) -> None:
    workflow = _load_module()
    design = _write(tmp_path / "feature_card.md", VALID_F0_CARD)

    exit_code = workflow.main(["validate", "--design", str(design), "--tier", "F0"])
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert "Feature workflow validation: PASS" in stdout
    assert "tier=F0 design_items=2 matrix_rows=2 warnings=0" in stdout
    assert "findings" not in stdout
    assert "implementation_refs" not in stdout

