from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_qe_create_page_uses_profile_and_semantic_universe_controls() -> None:
    source = (ROOT / "frontend/src/app/quantevolver/compose/page.tsx").read_text(
        encoding="utf-8"
    )

    assert "/quantevolver/dataset-profile" in source
    assert 'data-testid="qe-dataset-universe-controls"' in source
    assert 'data-testid="qe-dataset-execution-node"' in source
    assert 'data-testid="qe-universe-separate-runs"' in source
    assert "/quantevolver/evolution/universe-comparison-tasks" in source
    assert "universe_selection: { mode: universeMode, pool_ids: universePoolIds }" in source
    assert "provider_uri" not in source
    assert "instruments_sha256" not in source
    assert "coverage_receipt_sha256" not in source


def test_multi_alpha_create_page_reads_active_oos_defaults() -> None:
    source = (
        ROOT
        / "frontend/src/app/quantevolver/evolution/components/MultiAlphaCreateComposer.tsx"
    ).read_text(encoding="utf-8")

    assert "/quantevolver/dataset-profile" in source
    assert "payload.data.defaults.test_start" in source
    assert "payload.data.defaults.backtest_end" in source
    assert "datasetProfileError" in source
    assert "datasetProfileReady" in source
    assert "QE数据集配置不可用" in source
