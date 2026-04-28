from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = (
    REPO_ROOT / "backend" / "routers",
    REPO_ROOT / "backend" / "services",
)


def _production_python_files() -> list[Path]:
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
