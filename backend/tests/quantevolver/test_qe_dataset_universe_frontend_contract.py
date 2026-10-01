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


def test_qe_ui_exposes_star50_top20_and_comparison_truth() -> None:
    compose = (ROOT / "frontend/src/app/quantevolver/compose/page.tsx").read_text(
        encoding="utf-8"
    )
    comparison = (
        ROOT
        / "frontend/src/app/quantevolver/evolution/components/LoopMetricsComparison.tsx"
    ).read_text(encoding="utf-8")
    trajectory = (
        ROOT / "frontend/src/app/quantevolver/components/EvolutionTrajectory.tsx"
    ).read_text(encoding="utf-8")
    page = (ROOT / "frontend/src/app/quantevolver/evolution/page.tsx").read_text(
        encoding="utf-8"
    )

    assert 'poolId === "star50" ? 20 : topk' in compose
    assert "科创50单指数实验只允许 TopK=20" in compose
    assert "HMM 请求/启用/生效/触发" in comparison
    assert "黑名单 请求/启用/生效/动作" in comparison
    assert "基准年化" in comparison
    assert "超额年化" in comparison
    assert "RankICIR" in comparison
    assert "NON_SCALAR_METRIC_BRANCHES" in comparison
    assert 'absoluteSource === "enhanced_metrics.absolute_returns"' in comparison
    assert "hasAuthoritativeAbsoluteMetrics(loop, diagnostics)" in comparison
    assert "pool:${item.poolLabel}" in trajectory
    assert "modelParams?._qe_direct_v2_dataset_binding?.selection_pins" in trajectory
    assert "sharpe: metricsSummary.sharpe ?? metricsSummary.information_ratio" not in page


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
