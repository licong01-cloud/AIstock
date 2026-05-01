from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = (
    REPO_ROOT / "backend" / "routers",
    REPO_ROOT / "backend" / "services",
)
QUANTEVOLVER_FRONTEND_ROOT = REPO_ROOT / "frontend" / "src" / "app" / "quantevolver"
FACTOR_RULE_INDEX = REPO_ROOT / "backend" / "rating_rules" / "factor" / "index.json"


def _production_python_files() -> list[Path]:
    rel_roots = [root.relative_to(REPO_ROOT).as_posix() for root in PRODUCTION_ROOTS]
    try:
        tracked = subprocess.check_output(
            ["git", "ls-files", "--", *rel_roots],
            cwd=REPO_ROOT,
            text=True,
            encoding="utf-8",
        ).splitlines()
        return sorted(
            REPO_ROOT / rel
            for rel in tracked
            if rel.endswith(".py") and "__pycache__" not in Path(rel).parts
        )
    except Exception:
        pass

    files: list[Path] = []
    for root in PRODUCTION_ROOTS:
        files.extend(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)
    return sorted(files)


def test_production_factor_metrics_reads_are_calc_engine_scoped() -> None:
    """Runtime reads from the independent metrics table must not mix old engines."""
    bad_locations: list[str] = []
    source_pattern = re.compile(r"\b(FROM|JOIN)\s+aistock_factor_metrics\b", re.IGNORECASE)

    for path in _production_python_files():
        lines = path.read_text(encoding="utf-8").splitlines()
        for idx, line in enumerate(lines):
            upper_line = line.upper()
            if "DELETE FROM AISTOCK_FACTOR_METRICS" in upper_line:
                continue
            if not source_pattern.search(line):
                continue

            window = "\n".join(lines[max(0, idx - 30): min(len(lines), idx + 16)])
            if "calc_engine" not in window:
                rel = path.relative_to(REPO_ROOT).as_posix()
                bad_locations.append(f"{rel}:{idx + 1}")

    assert not bad_locations, "Missing calc_engine scope: " + ", ".join(bad_locations)


def test_cleanup_and_deletion_do_not_pin_fixed_rating_rule_version() -> None:
    checked_paths = (
        REPO_ROOT / "backend" / "services" / "quantevolver" / "deletion_candidate_service.py",
        REPO_ROOT / "backend" / "services" / "quantevolver" / "factor_cleanup_service.py",
    )

    for path in checked_paths:
        text = path.read_text(encoding="utf-8")
        assert "rule_version = 'v2.0.0'" not in text
        assert 'rule_version = "v2.0.0"' not in text


def test_factor_rating_rule_index_archives_v1_and_defaults_to_v2() -> None:
    data = json.loads(FACTOR_RULE_INDEX.read_text(encoding="utf-8"))
    versions = {item["version"]: item for item in data["versions"]}

    assert data["active_version"] == "v2.0.0"
    assert data["default_version"] == "v2.0.0"
    assert versions["v2.0.0"]["status"] == "active"
    assert versions["v1.0.0"]["status"] == "archived"


def test_factor_rating_service_rejects_non_v2_rule_execution() -> None:
    from backend.services.quantevolver.factor_rating_service import FactorRatingService

    svc = FactorRatingService()
    with pytest.raises(ValueError, match="only v2 factor rating rules are executable"):
        svc._grade_factor(
            {"id": 1, "factor_name": "dummy_factor", "source": "manual"},
            {"rule_version": "v1.0.0", "spec": {}, "grade_bands": {}},
            enable_llm_audit=False,
        )


def test_factor_analyst_does_not_default_unknown_category_to_tech() -> None:
    text = (REPO_ROOT / "backend" / "services" / "quantevolver" / "factor_analyst.py").read_text(
        encoding="utf-8"
    )
    assert "refusing default TECH fallback" in text
    assert 'category = "TECH"\n            classification_reason' not in text


def test_factor_analyst_rule_only_unknown_category_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.services.quantevolver import factor_analyst as module

    monkeypatch.setattr(module, "_get_official_grade", lambda factor_name: None)
    monkeypatch.setattr(module, "_classify_by_rules", lambda *args, **kwargs: (None, "no rule match"))
    monkeypatch.setattr(
        module.FactorAnalyst,
        "_get_factor_info",
        lambda self, factor_name, factor_source: {"expression": "unknown_expr", "code_text": "value = close"},
    )
    monkeypatch.setattr(module.FactorAnalyst, "_get_independent_metrics", lambda self, factor_name: {})
    monkeypatch.setattr(module.FactorAnalyst, "_get_multi_window_metrics", lambda self, factor_name: {})

    analyst = module.FactorAnalyst()
    with pytest.raises(ValueError, match="refusing default TECH fallback"):
        analyst.analyze_single_factor("unknown_factor_without_rule", "manual", use_llm=False)


def test_quantevolver_ui_marks_archived_rating_rules_non_executable() -> None:
    text = (QUANTEVOLVER_FRONTEND_ROOT / "components" / "FactorList.tsx").read_text(encoding="utf-8")
    assert 'rule.status === "archived"' in text
    assert "已归档/不可执行" in text
    assert "归档或非 v2 规则不可激活/执行" in text


def test_production_runtime_code_has_no_local_path_or_secret_fallbacks() -> None:
    banned_tokens = (
        "F:/Dev",
        "F:\\Dev",
        "/mnt/f",
        "/home/lc999",
        "lc78080808",
        "TDX_DB_PASSWORD:-",
        "DB_PASSWORD:-",
    )
    bad_locations: list[str] = []

    for path in _production_python_files():
        text = path.read_text(encoding="utf-8")
        for token in banned_tokens:
            if token in text:
                rel = path.relative_to(REPO_ROOT).as_posix()
                bad_locations.append(f"{rel}: contains {token!r}")

    assert not bad_locations, "Hardcoded local path/secret fallback: " + ", ".join(bad_locations)


def test_factor_production_code_does_not_read_classification_grade() -> None:
    """Current factor rating must come from active qe_factor_official_ratings."""
    bad_locations: list[str] = []
    alias_grade_pattern = re.compile(r"\b(?:c|cl|fc)\.grade\b", re.IGNORECASE)
    select_grade_pattern = re.compile(r"\bSELECT\s+grade\b", re.IGNORECASE)

    for path in _production_python_files():
        lines = path.read_text(encoding="utf-8").splitlines()
        for idx, line in enumerate(lines):
            if "legacy_grade" in line:
                continue
            if "grade_reason" in line:
                continue
            if alias_grade_pattern.search(line) or select_grade_pattern.search(line):
                window = "\n".join(lines[max(0, idx - 4): min(len(lines), idx + 8)])
                if "qe_factor_classification" in window or alias_grade_pattern.search(line):
                    rel = path.relative_to(REPO_ROOT).as_posix()
                    bad_locations.append(f"{rel}:{idx + 1}")

    assert not bad_locations, "Production reads classification.grade: " + ", ".join(bad_locations)


def test_quantevolver_ui_does_not_reference_legacy_factor_metric_fields() -> None:
    checked_paths = (
        QUANTEVOLVER_FRONTEND_ROOT / "compose" / "page.tsx",
        QUANTEVOLVER_FRONTEND_ROOT / "components" / "FactorAnalysisPanel.tsx",
        QUANTEVOLVER_FRONTEND_ROOT / "components" / "FactorList.tsx",
        QUANTEVOLVER_FRONTEND_ROOT / "components" / "ManualFactorDialog.tsx",
        QUANTEVOLVER_FRONTEND_ROOT / "components" / "MultiAlphaGroupEditor.tsx",
        QUANTEVOLVER_FRONTEND_ROOT / "factor-correlation" / "components" / "PairDetail.tsx",
        QUANTEVOLVER_FRONTEND_ROOT / "factor-deletion" / "page.tsx",
    )
    banned_patterns = (
        re.compile(r"\bic_value\b"),
        re.compile(r"\bsharpe_value\b"),
        re.compile(r"\bann_ret_value\b"),
        re.compile(r"\bv2_grade\b"),
        re.compile(r"\bv2_score\b"),
        re.compile(r"\b(?:f|fac|cls|cf|factor|classification|detail)\.grade\b"),
    )
    bad_locations: list[str] = []

    for path in checked_paths:
        lines = path.read_text(encoding="utf-8").splitlines()
        for idx, line in enumerate(lines):
            for pattern in banned_patterns:
                if pattern.search(line):
                    rel = path.relative_to(REPO_ROOT).as_posix()
                    bad_locations.append(f"{rel}:{idx + 1}")
                    break

    assert not bad_locations, "Quantevolver UI uses legacy factor fields: " + ", ".join(bad_locations)
