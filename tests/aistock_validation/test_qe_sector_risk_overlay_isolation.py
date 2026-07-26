from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_sector_risk_overlay_is_qe_only_and_has_no_gpu_telemetry() -> None:
    implementation_files = [
        ROOT / "backend/services/quantevolver/sector_risk_overlay.py",
        ROOT / "backend/services/quantevolver/sector_risk_overlay_evaluation.py",
        ROOT / "scripts/build_qe_sector_risk_overlay.py",
        ROOT / "scripts/qe_sector_risk_overlay.py",
        ROOT / "scripts/qe_sector_risk_overlay_strategy.py",
        ROOT / "scripts/qe_sector_risk_overlay_artifacts.py",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in implementation_files)
    forbidden_runtime_imports = (
        "backend.services.paper_trading",
        "backend.services.selection",
        "backend.services.advisory",
        "backend.infra.qmt",
        "xtquant",
    )
    for forbidden in forbidden_runtime_imports:
        assert forbidden not in combined
    for telemetry in ("nvidia-smi", "pynvml", "NVML"):
        assert telemetry not in combined


def test_overlay_builder_reads_only_explicit_qe_snapshot_files() -> None:
    source = (ROOT / "scripts/build_qe_sector_risk_overlay.py").read_text(encoding="utf-8")
    assert 'allowed_files=("daily_pv.h5", "sector_data.h5")' in source
    assert "daily_basic.h5" not in source
    assert "moneyflow.h5" not in source
