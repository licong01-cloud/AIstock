from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import research_assistant_real_model_smoke as smoke  # noqa: E402


def test_real_model_smoke_loud_skips_when_deepseek_key_missing(tmp_path: Path) -> None:
    output = tmp_path / "ra_real_model_smoke.json"
    env = os.environ.copy()
    env.pop("DEEPSEEK_API_KEY", None)
    env["PYTHONPATH"] = str(ROOT)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/research_assistant_real_model_smoke.py",
            "--output",
            str(output),
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 77
    assert output.exists()
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["bug_id"] == "BUG-496"
    assert payload["related_bug_ids"] == ["BUG-436", "BUG-496", "BUG-505", "BUG-509"]
    assert payload["acceptance_source"] == "B2/#1504 design killer assertions"
    assert len(payload["assertion_manifest"]) == 7
    assert payload["fake_pass"] is False
    assert payload["status"] == "skipped"
    assert payload["reason_code"] == "deepseek_api_key_missing"
    assert payload["llm_config"]["credential_source"] == "missing"
    assert payload["llm_config"]["db_config_lookup_allowed"] is False
    assert payload["safety"]["production_db_touched"] is False
    assert payload["safety"]["ddl_executed"] is False
    assert payload["safety"]["started_services"] is False
    combined_output = result.stdout + result.stderr
    assert "DEEPSEEK_API_KEY" in combined_output
    assert "fake_pass=false" in combined_output


def test_b2_tool_assertion_fails_on_missing_required_tool() -> None:
    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_tool_refs_present(  # noqa: SLF001 - script helper is the target under test.
            {("aistock-qe", "qe_archive_query_promotion_candidates")},
            smoke.B2_QE_REQUIRED_TOOLS,
            reason_code="missing_tools",
            label="unit smoke",
        )

    assert excinfo.value.reason_code == "missing_tools"
    assert "aistock-trading-ops/strategy_governance_list_packages" in excinfo.value.details["missing"]
    assert "aistock-trading-ops/strategy_governance_get_paper_readiness" in excinfo.value.details["missing"]


def test_b2_future_boundary_rejects_directional_prediction() -> None:
    text = "结论：驱动是业绩，情景是修复，风险是回撤；不构成投资建议，但我判断未来会上涨。"

    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_future_direction_boundary(text, label="unit future")  # noqa: SLF001

    assert excinfo.value.reason_code == "b2_future_directional_prediction_present"
    assert "会上涨" in excinfo.value.details["directional_markers"]


def test_b2_future_boundary_allows_negated_directional_terms() -> None:
    smoke._assert_future_direction_boundary(  # noqa: SLF001
        "结论：只谈驱动、情景和风险；不做方向预测，不给目标价，不构成投资建议。",
        label="unit future",
    )


def test_b2_template_marker_assertion_rejects_query_completed_reply() -> None:
    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_text_excludes(  # noqa: SLF001
            "已完成查询：工具1返回如下结果。",
            smoke.B2_FORBIDDEN_TEMPLATE_MARKERS,
            reason_code="template_marker",
            label="unit template",
        )

    assert excinfo.value.reason_code == "template_marker"
    assert "已完成查询" in excinfo.value.details["forbidden_markers"]


def test_b2_parse_args_defaults_expose_all_killer_assertion_messages() -> None:
    args = smoke.parse_args([])

    assert args.b2_qe_message == "QE成果怎么利用"
    assert "stock_analysis_get_quote" in args.b2_stock_message
    assert "external_research_search_web" in args.b2_stock_message
    assert "不做方向预测" in args.b2_future_message
    assert "风险" in args.b2_specificity_first_message
    assert "驱动" in args.b2_specificity_second_message
    assert "qe_template_create" in args.b2_write_message
    assert "000688" in args.b509_stock_depth_message
    assert "60" in args.b509_stock_depth_message



def test_b505_long_answer_completion_assertion_rejects_mid_sentence_tail() -> None:
    section_text = " ".join(" ".join(group) for group in smoke.B505_LONG_ANSWER_SECTION_TERMS)
    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_long_answer_complete(  # noqa: SLF001
            ("Bottom-line: " + section_text + ". Detailed analysis body with enough content. ") * 20
            + "unfinished tail",
            label="unit long answer",
        )

    assert excinfo.value.reason_code == "b505_long_answer_incomplete_tail"


def test_b505_parse_args_defaults_expose_long_answer_message() -> None:
    args = smoke.parse_args([])

    assert "Bottom-line" in args.b505_long_answer_message


def test_b509_stock_depth_tool_set_assertion_rejects_missing_web() -> None:
    tool_sets = {
        "function_registry": [f"{server}/{tool}" for server, tool in smoke.B509_STOCK_DEPTH_EXECUTABLE_TOOLS - {("aistock-external-research", "external_research_search_web")}],
        "executable": [f"{server}/{tool}" for server, tool in smoke.B509_STOCK_DEPTH_EXECUTABLE_TOOLS],
        "read_only_executable": [f"{server}/{tool}" for server, tool in smoke.B509_STOCK_DEPTH_EXECUTABLE_TOOLS],
        "function_registry_equals_read_only_executable": False,
    }

    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_b509_tool_sets(tool_sets)  # noqa: SLF001

    assert excinfo.value.reason_code == "b509_stock_depth_tool_refs_missing"
    assert "aistock-external-research/external_research_search_web" in excinfo.value.details["missing_from_function_registry"]


def test_b509_stock_depth_category_assertion_requires_core_dimensions() -> None:
    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_stock_depth_categories(  # noqa: SLF001
            {
                ("aistock-stock-analysis", "stock_analysis_get_quote"),
                ("aistock-stock-analysis", "stock_analysis_get_kline"),
                ("aistock-external-research", "external_research_search_web"),
            },
            label="unit stock depth",
        )

    assert excinfo.value.reason_code == "b509_stock_depth_required_categories_missing"
    assert "fund_flow" in excinfo.value.details["missing_categories"]
    assert "fundamental" in excinfo.value.details["missing_categories"]


def test_b509_stock_depth_tool_count_assertion_rejects_underuse() -> None:
    with pytest.raises(smoke.SmokeFailure) as excinfo:
        smoke._assert_tool_ref_count_at_least(  # noqa: SLF001
            {("aistock-stock-analysis", "stock_analysis_get_quote")},
            minimum=smoke.B509_STOCK_DEPTH_MIN_TOOL_EXECUTIONS,
            reason_code="too_few",
            label="unit stock depth",
        )

    assert excinfo.value.reason_code == "too_few"
    assert excinfo.value.details["minimum"] == 8
