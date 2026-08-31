from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
SOURCE = (
    ROOT
    / "scripts/qe_alpha_candidates/sector_rotation/dynamic_residual_flow_relation_v1.py"
)
SPEC = importlib.util.spec_from_file_location(
    "dynamic_residual_flow_relation_v1", SOURCE
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _panel(days: int = 280, seed: int = 17) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=days)
    sectors = np.array([101, 202, 303, 404, 505, 606], dtype=np.int32)
    rng = np.random.default_rng(seed)
    driver = rng.normal(0.0, 0.012, size=days)
    returns = rng.normal(0.0, 0.004, size=(days, len(sectors)))
    returns[:, 0] += driver
    returns[1:, 1] += 0.85 * driver[:-1]
    returns[5:, 2] += 0.75 * driver[:-5]
    flow = rng.normal(0.0, 0.02, size=(days, len(sectors)))
    flow[:, 0] += driver * 2.0
    residual = returns - returns.mean(axis=1, keepdims=True)
    flow_state = flow - flow.mean(axis=1, keepdims=True)
    return_rank = pd.DataFrame(residual).rank(axis=1, method="average", pct=True)
    flow_rank = pd.DataFrame(flow_state).rank(axis=1, method="average", pct=True)
    leadership = ((return_rank + flow_rank) / 2.0 - 0.5).to_numpy()

    index = pd.MultiIndex.from_product(
        [dates, sectors], names=["datetime", "l2_code_id"]
    )
    return pd.DataFrame(
        {
            "residual_return": residual.reshape(-1),
            "flow_state": flow_state.reshape(-1),
            "leadership_state": leadership.reshape(-1),
        },
        index=index,
    )


def _topology(panel: pd.DataFrame) -> pd.DataFrame:
    dates = panel.index.get_level_values("datetime").unique()
    return MODULE.fit_frozen_topology(
        panel,
        fit_start=dates[0],
        fit_end=dates[199],
        lags=(1, 5, 10, 20),
        top_k=2,
        min_observations=60,
    )


def test_fit_freezes_one_lag_per_directed_channel_edge() -> None:
    topology = _topology(_panel())

    assert not topology.empty
    assert not topology.duplicated(
        ["source_l2_code_id", "target_l2_code_id", "channel"]
    ).any()
    assert topology.groupby(["target_l2_code_id", "channel"]).size().max() <= 2
    assert set(topology["lag_days"]).issubset({1, 5, 10, 20})
    assert (topology["source_l2_code_id"] != topology["target_l2_code_id"]).all()
    assert (topology["selection_score"] > 0).all()


def test_known_lagged_relation_is_discovered() -> None:
    topology = _topology(_panel())
    relation = topology.loc[
        (topology["source_l2_code_id"] == 101)
        & (topology["target_l2_code_id"] == 202)
        & (topology["channel"] == "residual_return")
    ]

    assert not relation.empty
    assert int(relation.iloc[0]["lag_days"]) == 1
    assert float(relation.iloc[0]["fit_corr"]) > 0.5


def test_future_rows_cannot_change_fitted_topology() -> None:
    panel = _panel()
    dates = panel.index.get_level_values("datetime").unique()
    baseline = _topology(panel)
    changed = panel.copy()
    future = changed.index.get_level_values("datetime") > dates[199]
    changed.loc[future, "residual_return"] *= -5.0
    changed.loc[future, "flow_state"] *= 5.0

    actual = _topology(changed)

    pd.testing.assert_frame_equal(actual, baseline)


def test_prediction_date_values_do_not_change_same_day_edge_weight() -> None:
    panel = _panel()
    dates = panel.index.get_level_values("datetime").unique()
    topology = _topology(panel)
    baseline = MODULE.materialize_dynamic_weights(
        panel,
        topology,
        evaluation_start=dates[220],
        evaluation_end=dates[240],
        rolling_window=80,
        min_observations=50,
    )
    changed = panel.copy()
    changed.loc[(dates[230], slice(None)), "residual_return"] = 0.4
    changed.loc[(dates[230], slice(None)), "flow_state"] = -0.9

    actual = MODULE.materialize_dynamic_weights(
        changed,
        topology,
        evaluation_start=dates[220],
        evaluation_end=dates[240],
        rolling_window=80,
        min_observations=50,
    )

    expected_day = baseline.loc[baseline["datetime"] == dates[230]].reset_index(
        drop=True
    )
    actual_day = actual.loc[actual["datetime"] == dates[230]].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_day, expected_day)


def test_dynamic_materialization_never_reselects_topology() -> None:
    panel = _panel()
    dates = panel.index.get_level_values("datetime").unique()
    topology = _topology(panel)

    weights = MODULE.materialize_dynamic_weights(
        panel,
        topology,
        evaluation_start=dates[220],
        evaluation_end=dates[250],
        rolling_window=80,
        min_observations=50,
    )

    expected_edges = set(
        topology[
            ["source_l2_code_id", "target_l2_code_id", "channel", "lag_days"]
        ].itertuples(index=False, name=None)
    )
    actual_edges = set(
        weights[
            ["source_l2_code_id", "target_l2_code_id", "channel", "lag_days"]
        ].itertuples(index=False, name=None)
    )
    assert actual_edges == expected_edges


def test_invalid_topology_contracts_fail_loud() -> None:
    panel = _panel()
    dates = panel.index.get_level_values("datetime").unique()
    topology = _topology(panel)
    kwargs = {
        "evaluation_start": dates[220],
        "evaluation_end": dates[250],
        "rolling_window": 80,
        "min_observations": 50,
    }

    invalid_rank = topology.copy()
    invalid_rank.loc[0, "topology_rank"] = 0
    with pytest.raises(ValueError, match="topology_rank must be a positive integer"):
        MODULE.materialize_dynamic_weights(panel, invalid_rank, **kwargs)

    invalid_corr = topology.copy()
    invalid_corr.loc[0, "fit_corr"] = np.inf
    with pytest.raises(ValueError, match="fit_corr must be finite"):
        MODULE.materialize_dynamic_weights(panel, invalid_corr, **kwargs)

    duplicate = pd.concat([topology, topology.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="one lag per directed channel edge"):
        MODULE.materialize_dynamic_weights(panel, duplicate, **kwargs)


def test_invalid_panel_contracts_fail_loud() -> None:
    panel = _panel()
    duplicate = pd.concat([panel, panel.iloc[[0]]])
    with pytest.raises(ValueError, match="index must be unique"):
        MODULE.build_channel_panels(duplicate)

    non_finite = panel.copy()
    non_finite.iloc[0, 0] = np.inf
    with pytest.raises(ValueError, match="non-finite"):
        MODULE.build_channel_panels(non_finite)


def test_panel_units_fail_loud() -> None:
    panel = _panel()
    percent_scaled = panel.copy()
    percent_scaled["residual_return"] *= 100.0
    with pytest.raises(ValueError, match="decimal-return units"):
        MODULE.build_channel_panels(percent_scaled)

    invalid_flow = panel.copy()
    invalid_flow.iloc[0, invalid_flow.columns.get_loc("flow_state")] = 2.1
    with pytest.raises(ValueError, match="bounded normalized ratio"):
        MODULE.build_channel_panels(invalid_flow)

    invalid_leadership = panel.copy()
    invalid_leadership.iloc[
        0, invalid_leadership.columns.get_loc("leadership_state")
    ] = 0.6
    with pytest.raises(ValueError, match="normalized to"):
        MODULE.build_channel_panels(invalid_leadership)


def test_dynamic_weights_fail_when_one_frozen_edge_has_no_coverage() -> None:
    panel = _panel()
    dates = panel.index.get_level_values("datetime").unique()
    topology = _topology(panel)
    first_edge = topology.iloc[0]
    broken = panel.copy()
    target = int(first_edge["target_l2_code_id"])
    after_fit = broken.index.get_level_values("datetime") > dates[199]
    on_target = broken.index.get_level_values("l2_code_id") == target
    broken.loc[after_fit & on_target, "residual_return"] = np.nan

    with pytest.raises(ValueError, match="every frozen topology edge"):
        MODULE.materialize_dynamic_weights(
            broken,
            topology,
            evaluation_start=dates[220],
            evaluation_end=dates[250],
            rolling_window=30,
            min_observations=20,
        )


def test_artifact_builder_writes_hash_bound_parquet_receipt(tmp_path: Path) -> None:
    panel = _panel()
    dates = panel.index.get_level_values("datetime").unique()
    panel_path = tmp_path / "panel.parquet"
    topology_path = tmp_path / "outputs" / "topology.parquet"
    weights_path = tmp_path / "outputs" / "weights.parquet"
    receipt_path = tmp_path / "outputs" / "receipt.parquet"
    panel.to_parquet(panel_path)

    MODULE.build_relation_artifacts(
        panel_path=panel_path,
        topology_path=topology_path,
        weights_path=weights_path,
        receipt_path=receipt_path,
        fit_start=str(dates[0].date()),
        fit_end=str(dates[199].date()),
        evaluation_start=str(dates[220].date()),
        evaluation_end=str(dates[250].date()),
        lags=(1, 5, 10, 20),
        top_k=2,
        min_observations=60,
        rolling_window=80,
    )

    receipt = pd.read_parquet(receipt_path).iloc[0]
    assert receipt["contract_version"] == MODULE.CONTRACT_VERSION
    assert receipt["role"] == "RELATION_PRIOR"
    assert receipt["panel_sha256"] == MODULE._sha256(panel_path)
    assert receipt["topology_sha256"] == MODULE._sha256(topology_path)
    assert receipt["weights_sha256"] == MODULE._sha256(weights_path)
    assert int(receipt["topology_rows"]) > 0
    assert int(receipt["weight_rows"]) > 0


def test_artifact_builder_rejects_evaluation_overlap_and_repo_outputs(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="fit_end must precede"):
        MODULE.build_relation_artifacts(
            panel_path=tmp_path / "panel.parquet",
            topology_path=tmp_path / "topology.parquet",
            weights_path=tmp_path / "weights.parquet",
            receipt_path=tmp_path / "receipt.parquet",
            fit_start="2024-01-01",
            fit_end="2024-06-30",
            evaluation_start="2024-06-30",
            evaluation_end="2024-12-31",
        )

    panel_path = tmp_path / "panel.parquet"
    _panel().to_parquet(panel_path)
    with pytest.raises(ValueError, match="outside the repository"):
        MODULE.build_relation_artifacts(
            panel_path=panel_path,
            topology_path=ROOT / "topology.parquet",
            weights_path=tmp_path / "weights.parquet",
            receipt_path=tmp_path / "receipt.parquet",
            fit_start="2024-01-01",
            fit_end="2024-06-28",
            evaluation_start="2024-07-01",
            evaluation_end="2024-12-31",
        )


def test_artifact_builder_rejects_input_output_alias(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    _panel().to_parquet(panel_path)

    with pytest.raises(ValueError, match="must be distinct"):
        MODULE.build_relation_artifacts(
            panel_path=panel_path,
            topology_path=panel_path,
            weights_path=tmp_path / "weights.parquet",
            receipt_path=tmp_path / "receipt.parquet",
            fit_start="2024-01-02",
            fit_end="2024-10-01",
            evaluation_start="2024-10-02",
            evaluation_end="2024-12-31",
            min_observations=60,
        )
