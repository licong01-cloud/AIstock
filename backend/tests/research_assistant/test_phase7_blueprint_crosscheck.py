from __future__ import annotations

from pathlib import Path

from scripts.research_assistant_phase7_crosscheck import parse_dai_sections, parse_traceability_matrix, run_crosscheck


BLUEPRINT = Path("docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md")
EXPECTED = Path("tests/aistock_validation/catalog/research_assistant_phase7_expected.yaml")


def test_phase7_crosscheck_enumerates_all_traceability_and_dai_rows() -> None:
    traceability_rows = parse_traceability_matrix(BLUEPRINT)
    dai_rows = parse_dai_sections(BLUEPRINT)

    assert len(traceability_rows) >= 17
    assert {row.dai_id for row in dai_rows} >= {
        "DAI-MEM-001",
        "DAI-GND-003",
        "DAI-QE-001",
        "DAI-CODE-001",
        "DAI-REPORT-001",
        "DAI-PORT-004",
    }


def test_phase7_crosscheck_manifest_classifies_dai_defects_and_phase_anchors() -> None:
    result = run_crosscheck(BLUEPRINT, EXPECTED, fail_on_drift=True)

    assert result["status"] == "passed"
    assert result["defect_classifications"] == 13
    assert result["phase_anchor_count"] == 7


def test_phase8_code_intelligence_dai_is_no_longer_future_pending() -> None:
    result = run_crosscheck(BLUEPRINT, EXPECTED, fail_on_drift=False)

    assert result["status"] == "passed"


def test_phase9_proactive_report_dai_is_no_longer_future_pending() -> None:
    result = run_crosscheck(BLUEPRINT, EXPECTED, fail_on_drift=False)

    assert result["status"] == "passed"


def test_phase10_reflection_card_dai_is_no_longer_future_pending() -> None:
    result = run_crosscheck(BLUEPRINT, EXPECTED, fail_on_drift=False)

    assert result["status"] == "passed"
